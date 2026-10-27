#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Launch one fresh-JVM, local-mode cancellation diagnostic; retain raw evidence."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def summarize(output, mode, filename="summary.json"):
    with (output / "events.tsv").open() as stream:
        events = list(csv.DictReader(stream, delimiter="\t"))

    def select(event, role):
        return [e for e in events if e["event"] == event and e["role"] == role]

    failures = select("driver_failure", "short")
    long_failures = select("driver_failure", "long")
    cancelled = mode.startswith("cancel")
    if failures or (cancelled != bool(long_failures) and mode != "short-alone"):
        raise RuntimeError("Unexpected query outcome; inspect events.tsv")
    if cancelled and "cancelled" not in long_failures[0]["value"].lower():
        raise RuntimeError("Long query failed for a reason other than cancellation")
    trigger = select("cancel_request", "long") or select("contention_trigger", "long")
    short_submit = select("submit", "short")
    summary = {
        "mode": mode,
        "analysis_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "native_cancel_received_ns": None,
        "native_compute_stopped_ns": None,
        "resource_reusable_ns": None,
        "limitations": [
            "Task end is a scheduler observation, not native compute-stop time.",
            "Process CPU includes driver, Tokio workers, and the short query.",
            "Memory counters are process/accounting observations, not proof of physical reuse.",
            "Native pool registry counters can disappear before all reservations return.",
        ],
    }
    if trigger:
        start_ns = int(trigger[0]["nano_time"])
        end = select("task_end_finish_epoch_ms", "long")
        if len(end) != 2 or any(int(e["nano_time"]) < start_ns for e in end):
            raise RuntimeError("Expected two long tasks finishing after the trigger")
        summary["trigger_to_last_long_task_end_observed_ms"] = (
            max(int(e["nano_time"]) for e in end) - start_ns
        ) / 1e6
        if long_failures:
            summary["cancel_to_driver_failure_ms"] = (
                int(long_failures[0]["nano_time"]) - start_ns
            ) / 1e6
        with (output / "samples.tsv").open() as stream:
            samples = [s for s in csv.DictReader(stream, delimiter="\t")
                       if int(s["nano_time"]) >= start_ns]
        summary["sampled_peak_long_task_accounted_bytes_after_trigger"] = max(
            (int(s["long_task_accounted_bytes"]) for s in samples), default=0
        )
        summary["sampled_long_task_accounted_byte_seconds_after_trigger"] = sum(
            int(a["long_task_accounted_bytes"]) *
            (int(b["nano_time"]) - int(a["nano_time"])) / 1e9
            for a, b in zip(samples, samples[1:])
        )
    if short_submit:
        launches = select("task_launch_epoch_ms", "short")
        success = select("driver_success", "short")
        if not launches or not success:
            raise RuntimeError("Short query did not finish successfully")
        summary["short_submit_to_first_task_launch_observed_ms"] = (
            min(int(e["nano_time"]) for e in launches) - int(short_submit[0]["nano_time"])
        ) / 1e6
        summary["short_submit_to_driver_success_ms"] = (
            int(success[0]["nano_time"]) - int(short_submit[0]["nano_time"])
        ) / 1e6
    (output / filename).write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument("data", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--engine", choices=["comet", "spark"], default="comet")
    parser.add_argument("--mode", choices=["cancel", "cancel-alone", "complete", "short-alone"],
                        default="cancel")
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--width", type=int, default=32)
    parser.add_argument("--batch", type=int, default=8192)
    parser.add_argument("--delay-ms", type=int, default=200)
    parser.add_argument("--artifacts", type=Path,
                        help="Frozen native library and reactor classes for one experiment variant")
    parser.add_argument("--trace", action="store_true",
                        help="Enable diagnostic lifecycle logging; keep off for timing trials")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        parser.error(f"Refusing to overwrite {output}")
    if args.rows <= 0 or args.width < 2 or args.batch <= 0 or args.delay_ms < 0:
        parser.error("Need positive rows/batch, width >= 2 and delay-ms >= 0")
    native = ROOT / "native/target/release" / (
        "libcomet.dylib" if sys.platform == "darwin" else "libcomet.so")
    spark_classes = ROOT / "spark/target/classes"
    common_classes = ROOT / "common/target/classes"
    manifest = None
    if args.artifacts:
        artifacts = args.artifacts.resolve()
        manifest = json.loads((artifacts / "artifact.json").read_text())
        native = artifacts / native.name
        spark_classes = artifacts / "spark-classes"
        common_classes = artifacts / "common-classes"
        for relative, expected in manifest["class_files"].items():
            if hashlib.sha256((spark_classes / relative).read_bytes()).hexdigest() != expected:
                parser.error(f"Frozen class changed: {relative}")
    classpath_file = ROOT / "spark/target/cancellation-classpath.txt"
    need_native = args.action == "run" and args.engine == "comet"
    if (need_native and not native.exists()) or not classpath_file.exists():
        parser.error("Build native/JVM code and the classpath first; see README.md")
    if need_native and not args.artifacts and native.stat().st_mtime < max(
        p.stat().st_mtime for p in (ROOT / "native").rglob("*.rs")
        if "target" not in p.parts
    ):
        parser.error("Native library predates source files; rebuild it")
    source = ROOT / "spark/src/test/scala/org/apache/spark/sql/benchmark/CometCancellationProbe.scala"
    compiled = ROOT / "spark/target/test-classes/org/apache/spark/sql/benchmark/CometCancellationProbe$.class"
    if not compiled.exists() or compiled.stat().st_mtime < source.stat().st_mtime:
        parser.error("Probe classes predate the source; rerun test-compile")
    # Reactor outputs precede external dependencies; never resolve Comet from ~/.m2.
    dependencies = [p for p in classpath_file.read_text().strip().split(os.pathsep)
                    if "/org/apache/datafusion/comet-" not in p]
    classpath = os.pathsep.join(str(p) for p in [
        ROOT / "spark/target/test-classes", spark_classes, common_classes, *dependencies])
    options = (ROOT / "pom.xml").read_text().split("<extraJavaTestArgs>", 1)[1].split(
        "</extraJavaTestArgs>", 1)[0].split()
    java = Path(os.environ["JAVA_HOME"]) / "bin/java"
    command = [str(java), "-Xmx3g", "-Xss4m", "-XX:ActiveProcessorCount=2", *options,
               "-Dtest.appender=console",
               f"-Dprobe.cancellationTrace={str(args.trace).lower()}",
               f"-Djava.library.path={native.parent}",
               f"-Dprobe.cancelDelayMs={args.delay_ms}", "-cp", classpath,
               "org.apache.spark.sql.benchmark.CometCancellationProbe", args.action,
               str(args.data.resolve()), str(output), args.engine, args.mode,
               str(args.rows), str(args.width), str(args.batch)]
    output.parent.mkdir(parents=True, exist_ok=True)
    log = output.with_suffix(".log")
    if log.exists():
        parser.error(f"Refusing to overwrite {log}")
    environment = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", COMET_WORKER_THREADS="2",
                       COMET_CONF_DIR=str(ROOT / "conf"))
    native_digest = None
    if need_native:
        with native.open("rb") as stream:
            native_digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if manifest and native_digest != manifest["native_sha256"]:
            parser.error("Frozen native library changed")
    metadata = {
        "arguments": {key: str(value) for key, value in vars(args).items()},
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "native_sha256": native_digest,
        "probe_source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "runner_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "command": command,
        "artifact_manifest": manifest,
    }
    with log.open("x") as stream:
        result = subprocess.run(command, cwd=ROOT, env=environment, stdout=stream,
                                stderr=subprocess.STDOUT, timeout=360)
    if output.exists():
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if result.returncode:
        raise SystemExit(f"Probe failed ({result.returncode}); see {log}")
    if args.action == "run":
        if args.trace:
            matches = re.findall(
                r"cancellation_trace plan=(\d+) phase=(\w+) nano_time=(\d+)",
                log.read_text())
            with (output / "lifecycle.tsv").open("w") as stream:
                stream.write("plan\tphase\tnano_time\n")
                stream.writelines("\t".join(row) + "\n" for row in matches)
        summarize(output, args.mode)


if __name__ == "__main__":
    main()
