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

# 2026-10-26：取消與 task slot 歸還的第一輪實驗

已在 `upstream/main` 的 `c4a0b1f2bce44ffa2bbeab3bbc1f715853734969`
重現：Comet 的 job 很快回報取消，但兩個長 task 繼續消耗資源，短 task 等待數秒。
本輪只新增量測程式，production engine 原始碼沒有修改，也尚未接 cancellation listener。

## 設定與有效性

- Apple M3 Pro、36 GiB RAM、arm64、OpenJDK 21.0.12、Spark 4.1.3。
- 本 worktree 的 release native build 與 JVM reactor build。
- `local[2]`、兩個 Tokio workers、3 GiB JVM heap、2 GiB off-heap pool、FAIR pools。
- 1,600 萬列、32 個非固定值 long 欄位，兩個 Parquet 檔，磁碟占用約 3.9 GiB。
- 長工作是 native scan 接 partition-local native sort，完整消費所有列。
  Comet 計畫已斷言包含 `CometNativeScanExec` 與 `CometSortExec`，且固定兩個 task。
- 兩個 consumer 都進入後等待 200 ms 才取消，兩個 sort 都必須尚未輸出第一列。
- 短工作是 10,000 列的 sum。Spark 重用暖機後的 shuffle output，量測時是一個
  aggregate result task，因此這裡量的是一個短 task 獲派 slot 的等待。
- 14 輪有效實驗；各輪暖機都讀完 1,600 萬列，第一欄 XOR 都是
  `170321032354260451`，短工作結果都是 `49995000`。
  各輪 Scala probe 雜湊一致；所有 Comet 輪次的 native library 雜湊一致。

## 取消後的觀察

以下是每組三次的範圍。TaskEnd 與 launch 使用 listener 收到事件時的 monotonic
時間，包含通知延遲；它們不是 native 計算停止或實際記憶體歸還的時間。

| 引擎／batch | 取消到 driver 回報失敗 | 取消到最後 TaskEnd 通知 | 短工作提交到首個 launch 通知 |
| --- | ---: | ---: | ---: |
| Comet／8192 | 5.2–6.9 ms | 9.68–14.85 s | 9.27–14.66 s |
| Spark／8192 | 56.0–61.2 ms | 85.7–104.1 ms | 77.0–101.8 ms |
| Comet／1024 | 5.2–5.8 ms | 20.41–25.37 s | 20.19–24.33 s |

六次有競爭短工作的 Comet 取消實驗，兩個長 task 都仍消費完各自的 800 萬列。
預設 batch 三次在取消後還等了 6.37–10.94 s 才看到長 sort 的第一列輸出。

## 控制組

| 控制 | 觀察 |
| --- | --- |
| 短工作單獨執行，Comet | 8.7 ms 獲派通知，12.6 ms 完成 |
| 短工作單獨執行，Spark | 9.1 ms 獲派通知，12.6 ms 完成 |
| 不取消，Comet／8192 | 同樣觸發點到最後 TaskEnd 通知 16.03 s |
| 不取消，Comet／1024 | 同樣觸發點到最後 TaskEnd 通知 27.35 s |
| 取消但沒有短工作，Comet／8192 | driver 5.0 ms 回報失敗，最後 TaskEnd 通知 14.80 s |

沒有短工作競爭的取消輪次也讀完 1,600 萬列。涵蓋取消到 task completion 的相鄰
取樣區間約 14.80 s，整個 JVM process 增加約 9.89 CPU-s。這包含 driver、Tokio、
GC 等所有 process 工作，不能直接當成 native sort 獨占的 CPU 時間。
同輪取消後長 task 的 Spark 帳面記憶體取樣峰值約 1.76 GiB，積分約 9.87 GiB·s。
這是 Spark 記憶體 accounting，不是 RSS，也不是 native ownership 歸還的精確時間。

小 batch 沒有在這組資料上改善取消反應。不取消控制也較慢，因此不能把 batch 的
差異單獨歸因於取消機制。每個不取消控制只有一輪，機器另有背景工作，尚未做獨立的
throughput 或 instrumentation overhead 研究。

## 限制與下一個問題

目前可以支持「job 取消回報與 slot 歸還相隔數秒」這個構造案例。
native 收到取消、native 計算停止、實際資源可重用的精確時間仍未量到。
因此還不能只憑這輪區分 H1 與 H2，也沒有證明自然 query mix 的服務傷害。
Hash shuffle、CPU-heavy 與 I/O-wait 的獨立對照、任意 UDF 停止界限皆未驗證。
下一個實驗應接最小 listener／token，加入 native 收訊、停止與 reservation 歸還事件，
再量剩餘延遲及正常查詢成本。

一輪曾出現約 244 ms 的 wall clock／monotonic 差異；所有結果已從原始事件重新用
monotonic 時間計算，原本的 `summary.json` 仍保留供稽核。正式使用每輪的
`summary-monotonic.json` 與總表 `comparison-monotonic.json`。
兩次 Spark 冒煙測試、一次 Comet fallback 啟動失敗，以及 200 萬列在觸發前已輸出的
輪次，均排除在以上 14 輪之外。

原始資料與重現 patch 見 [evidence.zip](evidence.zip)。Archive 的 `baseline/study/`
保存各輪 command、版本／雜湊、計畫、events、samples、原始與重新分析的摘要。
Native build 輸出保留於本機；已核對 library 雜湊與所有 Comet 輪次一致。
重跑方式見 [README.md](README.md)。

驗證：release native build 成功，root Maven reactor `test-compile` 成功，Spotless
檢查成功，Python 語法檢查與 `git diff --check` 通過。此為 standalone benchmark，
本輪沒有修改 production operators，也沒有跑完整 Spark SQL CI。後續原型與第二輪結果
另記於 [PHASE2-RESULTS.md](PHASE2-RESULTS.md)，不改寫本輪 raw。
