/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.benchmark

import java.io.{File, PrintWriter}
import java.lang.management.ManagementFactory
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.TaskContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.comet.{CometNativeScanExec, CometSortExec}
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.{CometConf, CometSparkSessionExtensions, Native}

/** Local-mode diagnostic, deliberately outside the timing-sensitive CI suites. */
object CometCancellationProbe {
  private val contexts = new ConcurrentHashMap[Long, TaskContext]()
  private val firstLongOutput = new AtomicBoolean(false)
  @volatile private var events: PrintWriter = _
  @volatile private var entered = new CountDownLatch(2)
  @volatile private var longEnded = new CountDownLatch(2)
  @volatile private var shortStarted = new CountDownLatch(1)

  private def emit(event: String, role: String, task: Long = -1, value: String = ""): Unit =
    events.synchronized {
      events.println(
        s"${System.nanoTime()}\t${System.currentTimeMillis()}\t$event\t$role\t$task\t" +
          value.replace('\n', ' ').replace('\t', ' '))
      events.flush()
    }

  private def consume(frame: DataFrame, role: String): Array[(Long, Long)] = {
    frame.queryExecution.toRdd
      .mapPartitions { rows =>
        val context = TaskContext.get()
        val id = context.taskAttemptId()
        if (role == "long") contexts.put(id, context)
        emit("task_enter", role, id)
        context.addTaskCompletionListener[Unit] { _ =>
          emit(
            "task_completion",
            role,
            id,
            context.taskMemoryManager().getMemoryConsumptionForThisTask.toString)
        }
        if (role == "long") entered.countDown()
        if (role == "short") shortStarted.countDown()
        var checksum = 0L
        var count = 0L
        if (rows.hasNext) {
          if (role == "long") firstLongOutput.set(true)
          emit("first_row", role, id)
        }
        while (rows.hasNext) {
          checksum ^= rows.next().getLong(0)
          count += 1
        }
        emit("partition_rows", role, id, count.toString)
        emit("partition_checksum", role, id, checksum.toString)
        Iterator.single((count, checksum))
      }
      .collect()
  }

  private def worker(role: String, spark: SparkSession, frame: DataFrame): Thread = {
    val thread = new Thread(
      () => {
        spark.sparkContext.setJobGroup(role, role, interruptOnCancel = true)
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", role)
        emit("submit", role)
        try {
          val result = consume(frame, role)
          if (role == "short") {
            require(result.toSeq == Seq((1L, 49995000L)), "Unexpected short query result")
          }
          emit("driver_success", role)
        } catch {
          case NonFatal(error) =>
            emit("driver_failure", role, value = error.toString)
        } finally {
          spark.sparkContext.clearJobGroup()
        }
      },
      s"cancellation-probe-$role")
    thread.setDaemon(true)
    thread
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 8, "prepare|run dataDir outDir comet|spark mode rows width batch")
    val Array(action, dataDir, outDir, engine, mode, rowsArg, widthArg, batchArg) = args
    require(Set("prepare", "run").contains(action))
    require(Set("comet", "spark").contains(engine))
    require(Set("cancel", "cancel-alone", "complete", "short-alone").contains(mode))
    val output = new File(outDir)
    require(output.mkdirs(), s"Output must be a new directory: $output")
    events = new PrintWriter(new File(output, "events.tsv"))
    events.println("nano_time\tepoch_ms\tevent\trole\ttask\tvalue")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CometCancellationProbe")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.debug.maxToStringFields", "1024")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.files.maxPartitionBytes", 8L << 30)
      .config("spark.sql.files.minPartitionNum", "2")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.local.dir", new File(output, "scratch").getAbsolutePath)
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", 2L << 30)
      .config(CometConf.COMET_ENABLED.key, "false")
      .config(CometConf.COMET_SHUFFLE_ENABLED.key, "false")
      .config(CometConf.COMET_BATCH_SIZE.key, batchArg)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if (sys.props.get("probe.cancellationTrace").contains("true")) {
      org.apache.logging.log4j.core.config.Configurator
        .setLevel("org.apache.comet.CometExecIterator", org.apache.logging.log4j.Level.DEBUG)
    }
    spark.sparkContext.addSparkListener(new SparkListener {
      private val stageRoles = new ConcurrentHashMap[Int, String]()
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        val role = Option(event.properties.getProperty("spark.jobGroup.id")).getOrElse("warmup")
        event.stageIds.foreach(id => stageRoles.put(id, role))
      }
      override def onTaskStart(event: SparkListenerTaskStart): Unit = {
        emit(
          "task_launch_epoch_ms",
          Option(stageRoles.get(event.stageId)).getOrElse("unknown"),
          event.taskInfo.taskId,
          event.taskInfo.launchTime.toString)
      }
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
        val id = event.taskInfo.taskId
        val role = Option(stageRoles.get(event.stageId)).getOrElse("unknown")
        emit("task_end_finish_epoch_ms", role, id, event.taskInfo.finishTime.toString)
        if (event.taskMetrics != null) {
          emit("task_cpu_ns_total", role, id, event.taskMetrics.executorCpuTime.toString)
          emit("task_run_ms_total", role, id, event.taskMetrics.executorRunTime.toString)
        }
        if (role == "long") {
          contexts.remove(id)
          longEnded.countDown()
        }
      }
    })
    val samplingFailure = new AtomicReference[Throwable]()
    var sampler: Thread = null
    try {
      if (action == "prepare") {
        val rows = rowsArg.toLong
        val width = widthArg.toInt
        require(rows > 0 && width >= 2)
        val columns = (0 until width).map(i => s"xxhash64(id, $i) AS c$i")
        spark.range(0, rows, 1, 2).selectExpr(columns: _*).write.parquet(s"$dataDir/long")
        spark.range(0, 10000, 1, 2).write.parquet(s"$dataDir/short")
        emit("prepared", "driver", value = s"rows=$rows width=$width")
        return
      }
      val useComet = engine == "comet"
      val native = if (useComet) Some(new Native()) else None
      val cpu = ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[com.sun.management.OperatingSystemMXBean]
      spark.conf.set(CometConf.COMET_ENABLED.key, useComet.toString)
      spark.conf.set(CometConf.COMET_EXEC_ENABLED.key, useComet.toString)
      val long = spark.read.parquet(s"$dataDir/long").sortWithinPartitions("c0")
      val short = spark.read.parquet(s"$dataDir/short").agg(sum(col("id")))
      val plan = long.queryExecution.executedPlan
      val planFile = new PrintWriter(new File(output, "plans.txt"))
      try {
        planFile.println(s"LONG\n$plan\nSHORT\n${short.queryExecution.executedPlan}")
        planFile.println(spark.sparkContext.getConf.toDebugString)
        planFile.println(spark.conf.getAll.toSeq.sorted.mkString("\n"))
      } finally planFile.close()
      if (useComet) {
        require(plan.find(_.isInstanceOf[CometSortExec]).isDefined, s"No native sort: $plan")
        require(
          plan.find(_.isInstanceOf[CometNativeScanExec]).isDefined,
          s"No native scan: $plan")
      } else {
        require(!plan.toString.contains("Comet"), s"Spark reference contains Comet: $plan")
      }
      require(long.queryExecution.toRdd.getNumPartitions == 2, "Need exactly two long tasks")
      require(long.schema.length == widthArg.toInt, "Unexpected input width")
      val warmLong = consume(long, "warmup-long")
      require(warmLong.map(_._1).sum == rowsArg.toLong, "Unexpected input row count")
      require(
        consume(short, "warmup-short").toSeq == Seq((1L, 49995000L)),
        "Unexpected short query warmup result")
      spark.sparkContext.listenerBus.waitUntilEmpty(30000)
      entered = new CountDownLatch(2)
      longEnded = new CountDownLatch(2)
      shortStarted = new CountDownLatch(1)
      val samples = new PrintWriter(new File(output, "samples.tsv"))
      samples.println(
        "nano_time\tepoch_ms\tprocess_cpu_ns\tnative_allocated\tpool_reserved\t" +
          "pools\tplans\tlong_task_accounted_bytes\tlong_tasks")
      sampler = new Thread(
        () => {
          try {
            while (!Thread.currentThread().isInterrupted) {
              val usage = native.map(_.getMemoryUsage()).getOrElse(Array.fill[Long](4)(-1))
              val held = contexts
                .values()
                .asScala
                .map(_.taskMemoryManager().getMemoryConsumptionForThisTask)
                .sum
              samples.println(
                s"${System.nanoTime()}\t${System.currentTimeMillis()}\t${cpu.getProcessCpuTime}\t" +
                  s"${usage.mkString("\t")}\t$held\t${contexts.size()}")
              samples.flush()
              Thread.sleep(20)
            }
          } catch {
            case _: InterruptedException =>
            case NonFatal(error) => samplingFailure.set(error)
          } finally samples.close()
        },
        "cancellation-probe-sampler")
      sampler.setDaemon(true)
      sampler.start()
      emit("measurement_start", "driver")
      val longThread = worker("long", spark, long)
      val shortThread = worker("short", spark, short)
      if (mode != "short-alone") {
        longThread.start()
        require(entered.await(30, TimeUnit.SECONDS), "Long tasks did not both enter")
        Thread.sleep(sys.props.getOrElse("probe.cancelDelayMs", "200").toLong)
        require(longThread.isAlive && contexts.size() == 2, "Long tasks ended before trigger")
        require(!firstLongOutput.get(), "Sort produced output before trigger; increase the input")
        if (useComet) {
          require(native.get.getMemoryUsage()(3) >= 2, "No live native plans at trigger")
        }
        if (mode.startsWith("cancel")) {
          emit("cancel_request", "long")
          spark.sparkContext.cancelJobGroup("long")
          emit("cancel_return", "long")
        } else emit("contention_trigger", "long")
      }
      if (mode != "cancel-alone") shortThread.start()
      longThread.join(120000)
      shortThread.join(120000)
      require(
        !longThread.isAlive && !shortThread.isAlive,
        "Query exceeded the diagnostic timeout")
      if (mode != "short-alone") {
        require(longEnded.await(120, TimeUnit.SECONDS), "Long tasks did not report TaskEnd")
      }
      spark.sparkContext.listenerBus.waitUntilEmpty(30000)
      require(contexts.isEmpty, "Long task cleanup did not finish")
      if (mode != "cancel-alone") require(shortStarted.getCount == 0, "Short task did not run")
      Thread.sleep(200)
      require(samplingFailure.get() == null, s"Sampling failed: ${samplingFailure.get()}")
      emit("measurement_end", "driver")
    } finally {
      if (sampler != null) {
        sampler.interrupt()
        sampler.join(10000)
      }
      spark.stop()
      events.close()
    }
  }
}
