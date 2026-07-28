# ClickHouse 26.3 Memory Leak Reproduction Example (`clickhouse-26.3-memory-leak-repro`)

> **Repository**: https://github.com/ygrzjh/clickhouse-26.3-memory-leak-repro

This directory contains a minimal, turn-key reproducible example for a severe memory leak bug in ClickHouse 26.3 during high-frequency temporary table DDL and `MOVE PARTITION` workloads.

---

## ⚡ Quick Start for ClickHouse Maintainers

```bash
# 1. Launch side-by-side containers (ClickHouse 26.3 vs 25.8)
docker-compose up -d

# 2. Run the automated reproduction script (10 threads, 900 seconds)
python3 reproduce_leak.py 10 900
```

---

## 📌 Issue Summary

Under continuous high-concurrency temporary table workloads—characterised by rapid `CREATE DATABASE`, `CREATE TABLE`, `INSERT`, `ALTER TABLE ADD COLUMN`, `SYSTEM STOP MERGES`, `ALTER TABLE MOVE PARTITION`, and `DROP TABLE SYNC` cycles—**ClickHouse 26.3 exhibits a severe, continuous memory leak**, climbing endlessly past **1.40 GB – 1.56 GB** (Cgroup memory) with a steady positive slope of **`+10.46 MB/min`**.

In contrast, under the exact same workload and configurations, **ClickHouse 25.8 is completely stable**, with memory naturally flattening and returning to a **~600 MB** plateau with a negative second-half slope of **`-16.67 MB/min`**.

Even when configuring:
1. `clean_deleted_touching_tables_interval = 10` (shortening DatabaseAtomic dropped table GC from 8 minutes to 10 seconds)
2. `database_atomic_wait_for_drop_and_detach_synchronously = 1` (forcing synchronous table drops)

The memory leak in ClickHouse 26.3 persists, proving it is an **internal C++ memory reference/pool regression introduced between 25.8 and 26.3**.

---

## 📊 Empirical 15-Minute Benchmark Data (10 Concurrent Pipeline Threads)

| Metric / Environment | ClickHouse 26.3 (`26.3.13.31`) - Regressed | ClickHouse 25.8 (`25.8.7.3`) - Stable | Regression Status |
| --- | --- | --- | --- |
| **`jemalloc.allocated` (T=0s $\rightarrow$ T=300s $\rightarrow$ T=900s)** | 645.53 MB $\rightarrow$ 984.09 MB $\rightarrow$ **1087.88 MB** | 574.63 MB $\rightarrow$ 768.70 MB $\rightarrow$ **603.19 MB** | 🚨 **26.3 Continues Climbing** |
| **Second-Half Heap Slope (T=300s ~ 900s)** | **`+10.46 MB/min` (Unbounded Rise)** | **`-16.67 MB/min` (Plateau / Converged)** | 🚨 **Severe Leak in 26.3** |
| **MemoryResident (RSS) (T=0s $\rightarrow$ 300s $\rightarrow$ 900s)** | 696.50 MB $\rightarrow$ 1049.14 MB $\rightarrow$ **1152.55 MB** | 559.75 MB $\rightarrow$ 733.59 MB $\rightarrow$ **607.14 MB** | 🚨 **26.3 RSS Exceeds 1.15 GB** |
| **Second-Half RSS Slope (T=300s ~ 900s)** | **`+10.42 MB/min`** | **`-12.74 MB/min`** | 🚨 **26.3 RSS Unbounded** |
| **Container Cgroup Memory (Peak)** | **1402.19 MB (Peak: 1566.01 MB)** | **811.90 MB (Peak: 964.39 MB)** | 🚨 **+590 MB Difference** |

---

## 🔍 Root Cause Analysis & C++ Allocation Stack Traces

Introspection stack trace analysis of `system.trace_log` (`trace_type IN ('Memory', 'MemoryPeak')`) on ClickHouse 26.3 identified two primary un-freed C++ allocation paths:

### 1. `MergeTreeDataPartWriterCompact` Primary Index Block Cloning (260.38 GiB Total Allocated)
```cpp
DB::Block::cloneEmptyColumns()
  <- DB::MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex(...)
  <- DB::MergeTreeDataPartWriterCompact::writeDataBlockPrimaryIndexAndSkipIndices(...)
  <- DB::MergeTreeDataPartWriterCompact::finalizeIndexGranularity()
  <- DB::MergeTreeDataWriter::writeTempPartImpl(...)
  <- DB::MergeTreeDataWriter::writeTempPart(...)
```
**Mechanism**: In 25.9+, `MergeTreeDataPartWriterCompact` introduced async primary index serialization with `cloneEmptyColumns()`. When temporary stage tables are rapidly created, written to, and dropped, primary index column buffers allocated by `writeTempPartImpl` remain tied to thread-local cache structures and are not freed when the temporary table is dropped.

### 2. `MOVE PARTITION` Asynchronous Part Finalization (`finalizePartAsync`) (73.09 GiB Total Allocated)
```cpp
DB::MergedBlockOutputStream::finalizePartAsync(...)
  <- DB::MergedBlockOutputStream::finalizePart(...)
  <- DB::MergeTreeData::createEmptyPart(...)
  <- DB::createEmptyDataParts(...)
  <- DB::StorageMergeTree::movePartitionToTable(...)
  <- DB::MergeTreeData::alterPartition(...)
```
**Mechanism**: In 26.3, `movePartitionToTable` invokes `finalizePartAsync`. Asynchronous empty part finalization creates background thread pool handles. When temporary tables are dropped with `DROP TABLE SYNC`, referenced part handles in global background thread pools fail to unregister synchronously, causing heap memory to climb endlessly.
