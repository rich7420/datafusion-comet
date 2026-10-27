<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 2026-10-26：批次檢查、native 取消訊號與 cleanup

本輪確認兩個不同的延遲來源：批次邊界檢查仍會等 native 首次輸出；接上可喚醒
native wait 的取消訊號後，JNI 可以很快返回，但 `releasePlan` 仍可能耗時一秒以上。
後者已在實際 sort workload 的 trace 中觀察到，尚未細分 cleanup 內部的時間。

這是本機實驗原型，不是可直接合併的修補。第一輪未改 production 的結果保留在
[RESULTS.md](RESULTS.md)。相關上游討論為
[#6295](https://github.com/apache/datafusion-comet/issues/6295)。

## 版本與量測方式

共同基底為 `c4a0b1f2bce44ffa2bbeab3bbc1f715853734969`。沿用第一輪的
1,600 萬列、32 個 long 欄位、兩個 Parquet 檔，及 Spark 4.1.3、JDK 21.0.12、
`local[2]`、兩個 Tokio workers、3 GiB heap、2 GiB off-heap、batch 8192。
Comet shuffle 關閉。長工作為 native scan 接 partition-local sort；短工作沿用暖機後
的 aggregate shuffle output，量測的是單一 result task 的 admission。

三組 production classes、native library 與 patch 各自凍結：

| 組別 | 行為 | native library SHA-256 |
| --- | --- | --- |
| baseline | 原版 `main` | `7d8e71e084b5a2dd37cbc2c8badfca84e48363e3523f73edb57ff3dbb66a8f06` |
| A / batch | 在 JNI callback 前後檢查 Spark task 是否被取消；保留 TaskKilledException | 與 baseline 相同 |
| B / native | A 加上每 25 ms 檢查 captured TaskContext 的 watcher，透過獨立 token handle 喚醒 native wait | `da7a84f6fe21803894c366e7ed40b76529ed4821dcf949d82026deb3c75d009c` |

Watcher 不跨執行緒存取正在執行的 ExecutionContext；cancel 與 token handle 釋放互斥。
Plan 的關閉仍在原 executor thread。Native-only receiver 與 JVM-fed wait 都加入取消路徑；
本輪 workload 只覆蓋 native-only scan/sort，JVM-fed 路徑另以 component tests 覆蓋。

13 輪共用相同新版 probe 與 runner，其 SHA 分別為：

- Probe：`d5f81ba152864b9597701a772e42cd60a30bde0bd3eae24e91104abf75ef967c`
- Runner：`e3a457cd7bc2dba8089af3452b09e041facedd8305a5c475e7635443afddea94`

Probe 改在 SparkListener 的 TaskEnd 才移除長 task 的記憶體取樣 context，並等待兩個
TaskEnd 後才結束取樣。Task completion callback 可能早於 native close，不能當作 cleanup
完成。第一輪 raw 不改寫；其取消工作最後仍讀完所有列，已有完整 TaskEnd 記錄。

## 關閉 trace 的正式輪次

除特別標示外，兩個長 consumer 都進入後 200 ms 提出取消，且都尚未輸出第一列。
TaskEnd／launch 欄位採 monotonic listener receipt，含通知延遲；consumer entry 由
executor 直接打點，也使用 monotonic 時間。以下不是統計性的尾延遲估計。

| 組別／輪次 | 取消至最後 TaskEnd 通知 | 短工作提交至 launch 通知 | 短工作提交至 executor consumer entry |
| --- | ---: | ---: | ---: |
| baseline / 01 | 7.418 s | 7.415 s | 7.417 s |
| A / 02 | 9.442 s | 9.324 s | 9.326 s |
| A / 06 | 8.633 s | 8.572 s | 8.574 s |
| B / 03 | 25.101 ms | 23.226 ms | 25.062 ms |
| B / 05 | 1.823 s | 1.820 s | 1.822 s |
| Spark / 04 | 69.900 ms | 68.016 ms | 69.854 ms |

所有取消輪次 driver 都回報取消；baseline 仍將兩個 800 萬列 partition 全部送到
consumer。A、B 與 Spark 的取消輪次沒有任何 first-row 或 partition_rows 成功事件。
這表示 consumer 沒收到第一列，不代表 native 在取消瞬間停止計算。

A 沒有解除等待 native 首次輸出的問題。A 與 baseline 的速度差異不能解讀成取消檢查
造成退化：樣本少，且各輪暖機耗時也有明顯變異。B 縮短了這些實驗中的 admission
等待，但兩輪仍從數十毫秒到 1.82 秒，不能承諾固定的取消上界。

## Trace：收到訊號後仍會等 cleanup

Trace 輪次分開執行，不混入上表的正式計時。下表所有時間均從提出取消開始計算，
取兩個長 plan 中相應事件的最晚時間。`cancel_ack` 是 native cancel 呼叫已返回，
不是 native 計算停止；`release_return` 也不是實體記憶體可重用的證明。

| 輪次 | 取消觸發延遲 | cancel_ack | execute_return | release_return |
| --- | ---: | ---: | ---: | ---: |
| A / 10 | 200 ms | 無此訊號 | 7,021.181 ms | 7,022.117 ms |
| B / 11 | 200 ms | 11.149 ms | 11.434 ms | 25.791 ms |
| B / 12 | 200 ms | 27.010 ms | 27.229 ms | 1,509.766 ms |
| B / 13 | 1,000 ms | 21.966 ms | 22.150 ms | 999.166 ms |

在 B / 12，兩個 plan 的 `releasePlan` 呼叫分別持續約 1.471、1.482 秒；短 task
直到提交後 1.502 秒才獲派。在 B / 13，兩個 release 呼叫持續約 970、977 ms。
因此至少這兩輪的主要剩餘延遲在 JNI wait 返回之後的 native cleanup 呼叫內。
尚未分辨 stream mutex 的 in-flight poll、stream drop、子 task 清理或 reservation 等待
各占多少；不能將整段直接歸因於 sort 的某個同步 poll。

A / 10 在首次 JNI 返回前等待約 6.87–7.02 秒，兩個 release 呼叫本身均少於 1 ms。
所以只加 JNI 邊界的 Spark kill check 無法讓這個等待中的 task 及早交出 slot。

這支持 H1 與 H2 可以先後出現：接通訊號能解除 native wait，但不能單憑訊號已送達
推論 teardown 已完成。後續若要改善 B 的慢輪次，應先細分 cleanup，無須再設計另一套
取消訊號框架。

## 控制與資源觀察

- A / 07 與 B / 08 不取消控制均成功讀完 1,600 萬列，第一欄 XOR 與暖機一致；短工作
  均得到 `49995000`。兩個控制各只有一輪，不能估計正常查詢的 watcher overhead。
- B / 09 取消且沒有短工作，最後 TaskEnd 約 30.855 ms；consumer 沒收到第一列。
- 13 輪暖機均讀完 1,600 萬列，第一欄 XOR 為 `170321032354260451`，短暖機結果正確。
- 沒有在 13 輪 log 中找到 non-zero memory、reservation timeout、cancel failure 或
  native panic 的對應警告。這不等同於證明沒有尚存的 native allocations。
- 關閉 trace 的競爭取消輪次，長 task 帳面記憶體積分：baseline 約 6.63 GiB·s，
  A 約 8.56–9.23 GiB·s，B 約 0.012–1.95 GiB·s。這是 Spark accounting 的取樣積分。
- 同輪取消到 TaskEnd 的取樣前後區間，process CPU 增量：baseline 約 9.30 CPU-s，
  A 約 5.27–5.29 CPU-s，B 約 0.17–1.09 CPU-s。區間包含相鄰取樣誤差、driver、
  Tokio、GC 與短工作，不能稱為取消後 native sort 的獨占 CPU。

## 驗證與界限

最終 release library 的 CometExecIteratorLifecycleSuite 與 CometNativeSuite 合計
37/37 通過，命令從 repository root 執行：

```sh
./mvnw -B -Prelease test -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsuites=org.apache.spark.CometExecIteratorLifecycleSuite,org.apache.comet.CometNativeSuite
```

已核對 Maven 包入的 library 與凍結 B binary 的 SHA 完全相同。Maven 預設使用 debug
library，因此以 `-Prelease` 的最終紀錄為準。
A 的三個新取消 regression 曾在未套用 A 時失敗，套用後同 suite 30/30 通過。
保留相同 JNI ABI、停用 native wait 的 cancellation select 時，五個新 Rust cancellation
測試全部失敗；恢復後完整 JNI module 48/48 通過。之後再強化「兩個分支均 ready」與
「真正 Pending 後才取消」的 fixture，最新 module 再跑仍是 48/48，release library 重建成功。
Nested callback 的 cancellation lost-wake 另有使用實際 helper 原始碼的紅綠檢查。

只執行了本機 Spark 4.1.3 的指定 suites 與 standalone workload；未跑完整 Spark SQL、
其他 Spark profiles、shuffle、I/O-wait 或任意 UDF 的停止測試。沒有量 natural query mix
的影響、實體資源重用時間，也沒有證明任意 callback 可在固定期限內終止。

原始資料與重現 patch 見 [evidence.zip](evidence.zip)。其中
`phase2/study/` 有每輪 raw events、samples、plans、metadata、summary，以及診斷輪次的
lifecycle.tsv；`phase2/artifacts/` 保存 manifest，production patches 另置於 `phase2/`。
`phase2/comparison.json` 由隨附的 `analyze.py` 重建。Archive 不含編譯產物與 3.9 GiB 資料集；
metadata 中的絕對路徑是原始執行紀錄，不是下載位置。完整 native/JVM build 與紅綠測試
log 保留於本機，未包入 archive。

重跑凍結版本的單輪命令如下；`--trace` 僅用於診斷輪次。`--artifacts` 會檢查 manifest
內的 class 與 library 雜湊；本機 artifact 路徑不是公開下載位置。

```sh
python3 benchmarks/cancellation/run.py run /scratch/data-16m-32 /scratch/native-cancel \
  --rows 16000000 --artifacts /scratch/artifacts/native
```
