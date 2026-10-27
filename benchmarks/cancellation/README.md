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

# Cancellation and slot return: first diagnostic

This standalone local-mode probe measures a native scan feeding a partition-local
sort, cancelled while both executor slots are occupied, followed by a short query.
The probe itself adds no production engine behavior. This research worktree also
contains separate cancellation prototypes; the original unchanged-engine study
is in [RESULTS.md](RESULTS.md), and the prototype comparison is in
[PHASE2-RESULTS.md](PHASE2-RESULTS.md). Related upstream work:
[#6261](https://github.com/apache/datafusion-comet/pull/6261) and
[#6295](https://github.com/apache/datafusion-comet/issues/6295).
Comet shuffle is disabled explicitly to isolate partition-local sort while using
Spark's default shuffle manager for the short aggregate.

## Observed results

In the second round, a short task submitted immediately after cancellation waited
for the following task-launch notifications. All runs used the same probe, data,
and resource configuration; debug tracing was off.

| Variant | Runs | Short submission to task-launch notification |
| --- | ---: | ---: |
| Unchanged engine | 1 | 7.415 s |
| A: Spark interruption checks around JNI | 2 | 8.572–9.324 s |
| B: A plus a native cancellation token and 25 ms watcher | 2 | 23.226 ms, 1.820 s |
| Spark reference | 1 | 68.016 ms |

A separate slow B diagnostic run returned from both `executePlan` calls within
27.3 ms after cancellation, but the subsequent `releasePlan` calls took
1.471–1.482 s. The short task's admission took 1.502 s. This locates the residual
delay inside the native cleanup call; it does not identify which cleanup step
dominates. A fast diagnostic run completed both release calls by 25.8 ms.

These are small constructed cases. The short query reuses warmed shuffle output,
so the observation concerns one result task's admission. Task-launch times include
listener notification delay. Normal-query watcher overhead, natural workload
impact, exact native compute-stop time and physical memory reuse are unmeasured.
The prototype has only been exercised locally with Spark 4.1.3, not across all
supported Spark profiles or the full Spark SQL suite.

See [the first report](RESULTS.md), [the prototype comparison](PHASE2-RESULTS.md),
and [raw evidence and reproducer patches](evidence.zip). The archive includes the
original 14 runs and 13 later runs, their separate probe versions, and manifests.
It excludes the data files and compiled binaries.

## Experimental snapshots

| Snapshot | Commit |
| --- | --- |
| Probe with unchanged production engine | [9c5f5d553](https://github.com/rich7420/datafusion-comet/tree/9c5f5d553) |
| A: interruption checks around JNI | [e83863572](https://github.com/rich7420/datafusion-comet/tree/e83863572) |
| B: native cancellation prototype, tests and results | This branch's tip |

The baseline and A snapshots use the second round's common probe. Rebuild native
and JVM code after switching snapshots, and keep each run's output directory
separate. All three snapshots are based on upstream `c4a0b1f2b`.

## Build and run

Requires Python 3.11 or newer (the recorded runs used Python 3.14) and JDK 21.
Build from the worktree root, without Maven `-pl`:

```sh
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
cargo build --manifest-path native/Cargo.toml --release
./mvnw -B -Prelease test-compile dependency:build-classpath -Dmdep.outputFile=target/cancellation-classpath.txt -Dmdep.includeScope=test -DskipTests
```

Use a scratch destination with enough space for uncompressible Parquet and fresh
output directories. Defaults generate 2M rows, 32 long columns and two files.
Every run warms the long and short paths once, then starts measurement. A run has
two Spark task slots, two Tokio workers, a 3 GiB JVM heap and 2 GiB off-heap pool.

```sh
python3 benchmarks/cancellation/run.py prepare /scratch/cancel/data /scratch/cancel/prepare --rows 16000000
python3 benchmarks/cancellation/run.py run /scratch/cancel/data /scratch/cancel/comet-1 --engine comet --rows 16000000
python3 benchmarks/cancellation/run.py run /scratch/cancel/data /scratch/cancel/spark-1 --engine spark --rows 16000000
python3 benchmarks/cancellation/run.py run /scratch/cancel/data /scratch/cancel/solo --mode short-alone --rows 16000000
python3 benchmarks/cancellation/run.py run /scratch/cancel/data /scratch/cancel/no-cancel --mode complete --rows 16000000
python3 benchmarks/cancellation/run.py run /scratch/cancel/data /scratch/cancel/no-short --mode cancel-alone --rows 16000000
```

Repeat engine/control pairs in alternating order; also run `--batch 1024` versus
the default 8192. `--delay-ms` is measured after both long tasks enter the consumer,
not from query submission. The probe rejects a run if either long task has already
ended or either sort has produced its first row at the trigger. Increase the
dataset if this happens. Cancellation uses
`setJobGroup(..., interruptOnCancel = true)` and `cancelJobGroup`.

Before measurement, the source-derived prediction is that shrinking the input
batch will not by itself create a cancellation check inside native sort. It can
change sort throughput and remaining work, but should not make cancellation
latency independent of the sort's time to its next output. Compare full-query
controls before attributing a smaller residual to better cancellation delivery.

The sink consumes every row with all columns present in the executed plan. Comet
runs assert native scan and sort nodes and exactly two long partitions. Each run
saves the plan, configuration, native library digest, command and source revision.
The Spark reference uses the same data and resource settings with Comet disabled.
Spark reuses the short frame's warmed shuffle output, so its measured work is a
single aggregate result task. This is a task-slot admission probe. A fresh scan's
end-to-end service time needs a separate experiment.

`events.tsv` records cancellation, driver failure, executor consumer entry and
completion, and task launch/end times. Listener receipt uses monotonic time;
Spark's own task launch/finish fields use epoch milliseconds and are kept separate.
Summary durations use monotonic listener receipt times, including listener delay,
so changes to the system wall clock do not change those durations.
`samples.tsv` samples process CPU, native allocator/pool counters, and long-task
Spark memory accounting every approximately 20 ms. `summary.json` reports job-failure
latency, last-long-task latency and short-query admission/completion times.
The Spark reference does not load the native library; its native counter columns
contain `-1` (unobserved).

Interpret the evidence narrowly:

- Native cancellation receipt, exact native computation stop and physical resource
  reusability are **unmeasured**. The summary leaves these fields null.
- Task completion callbacks precede final Spark task cleanup. The recorded task-end
  finish time is the scheduler observation; a short task launch directly observes
  admission to a slot. Neither proves native memory has been returned.
- Spark's `executorCpuTime` covers the task thread, excluding background Tokio CPU.
  Process CPU includes driver, Tokio workers and the competing short query. It
  cannot identify cancelled-query CPU by itself.
- Native pool/plan counters are process-wide. A pool unregisters before the final
  reservation wait in `release_plan`, so a zero counter alone does not prove all
  reservations have returned. Sampled task memory is Spark accounting, not RSS or
  an exact native reservation lifetime. Byte-seconds use left-rectangle sampling.
- This is a constructed saturated-slot case, not a natural workload or a tail-latency
  distribution. Other machine activity and instrumentation can affect timings.

The prototype comparison accepts `--artifacts PATH` for a previously frozen
library and reactor classes with an `artifact.json` manifest. It validates the
recorded class and library hashes. Each run records that manifest in metadata.
Use `--trace` only for separate diagnostic runs: it enables JVM lifecycle markers
and extracts them to `lifecycle.tsv`. These markers bracket cancellation and
release JNI calls; they do not measure exact native compute-stop or physical
reservation reuse. See the second report for the retained local snapshots.

Hash shuffle, I/O-wait paths and normal-query listener overhead remain separate
experiments. Cleanup needs finer instrumentation before assigning its residual
latency to a particular poll or ownership path.
