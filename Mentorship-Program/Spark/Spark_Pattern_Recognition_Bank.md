# Apache Spark — Pattern Recognition Bank

**Source:** Mentor's *Spark Top 50 Interview Questions* (Q1–Q7 reviewed); technical clarifications marked below.  
**Purpose:** Recognize the interview clue → retrieve the right Spark mechanisms → reconstruct your own answer. **This is not a second answer book.**  
**Progress:** Day 1, Q1–Q7 reviewed (7/50). Next: Q8–Q14.

## 1. 60-second recognition bank

| Q | Hear this interview clue | Instantly recall these keywords | Avoid this mix-up |
|---|---|---|---|
| **Q1** | **10 TB; 80% runtime in one wide transformation** | **Spark UI** → shuffle read/write → spill → uneven task times / **skew?** → partition count → `.explain()` (BHJ/SMJ) → AQE/skew join → predicate pushdown → built-ins instead of UDFs → remeasure | One slow stage does **not** prove skew; check task distribution. Predicate pushdown concerns **reading**, not writing Parquet. |
| **Q2** | **500M rows; `groupBy().agg()`; only 200 partitions** | **Shuffle partition count** → inspect actual *aggregated* partition sizes → `spark.sql.shuffle.partitions` → AQE *coalesces small* partitions → `repartition(n)` vs `coalesce(n)` → `write.partitionBy()` if relevant | 500M **input** rows do not mean 500M output rows. Memory doesn't cause the configured 200 partition count. |
| **Q3** | **Many `.cache()` calls; executor OOM** | **Reuse** > once → right storage level → `unpersist()` → **Storage tab** → `clearCache()` (test cleanup) → checkpoint if breaking long lineage is appropriate | Caching after a wide transformation isn't inherently wrong; unnecessary retained caches cause pressure. |
| **Q4** | **Delta query slow even after Z-ORDER** | **Min/max statistics → data skipping** → selective filter on Z-ordered column → not already clustered → low cardinality/small table limitations → **Liquid Clustering / partition pruning / Bloom filters** (when appropriate) | Z-ordering doesn't guarantee faster scans; filter selectivity and file-skipping opportunities matter. |
| **Q5** | **Executor GC > 30% of task time** | **Objects** allocated/retained → JVM heap pressure → **Executors tab** GC metrics → DataFrame/Tungsten → Kryo (relevant workloads) → executor heap/off-heap → G1GC → remeasure | Don't jump straight to AQE or partition count; first investigate object allocation and heap pressure. |
| **Q6** | **Read 1 TB JSON → transforms → write Parquet** | **R-S-E-W:** **Read** file splits/parallelism → **Shuffle** partitions/AQE/broadcast small lookups → **Executors** CPU/heap/local shuffle I/O/speculation → **Write** Parquet Snappy/ZSTD/file layout → validate | `spark.sql.files.maxPartitionBytes` = input **bytes**; `spark.sql.shuffle.partitions` = shuffle **count**. JSON splittability varies by encoding/compression. |
| **Q7** | **Tungsten vs RDD; why faster?** | **B-C-C:** **Binary** compact representation → CPU **Cache-aware** algorithms → whole-stage **Codegen** → less JVM object/operator overhead; limited with direct RDD APIs / Python or unsupported UDF paths | CPU cache locality ≠ `.cache()` persistence. Tungsten isn't always off-heap or columnar. |

**Fast answer pattern for scenario questions:** **Clue → Spark internal mechanism → Diagnose/verify → Targeted fix → Validate/trade-off.**

## 2. How the seven questions connect

```text
             1 TB JSON / Parquet pipeline (Q6)
                           |
                  READ + FILE LAYOUT
             Q6 (input splits) / Q4 (Z-order)
                           |
                  TRANSFORM + SHUFFLE
          Q1 (slow wide stage) / Q2 (200 partitions)
                           |
                   EXECUTION + MEMORY
      Q3 (retained cached data) / Q5 (JVM GC)
                           |
            Q7 (Tungsten reduces overhead)
                           |
                   WRITE + MEASURE
           Q6 (compression and output layout)
```

**Symptom recognition:**

- **Some tasks take 25 min; most take 2 min** → suspect **skew/stragglers**, validate with per-task shuffle size, duration and spill (Q1).
- **All aggregation tasks are huge; partition count is fixed at 200** → inspect **actual shuffle sizes** and tune **partitioning** (Q2).
- **Storage tab keeps filling after repeated use** → check **cached datasets**, reuse, and unpersist (Q3).
- **Filtered Delta scans still read most files** → check **data-skipping statistics and filter selectivity** (Q4).
- **GC time dominates execution** → inspect **JVM objects, heap pressure, and executor GC** (Q5).
- **End-to-end JSON-to-Parquet tuning** → **R → S → E → W** (Q6).
- **Asked why DataFrame execution may outperform object-heavy RDD execution** → **Tungsten B-C-C** (Q7).

## 3. Configuration anchors (recall exact names)

| What it controls | Exact key / API |
|---|---|
| File scan bytes per partition | `spark.sql.files.maxPartitionBytes` |
| Initial SQL shuffle partition count | `spark.sql.shuffle.partitions` |
| Adaptive Query Execution | `spark.sql.adaptive.enabled` |
| AQE skewed join optimization (supported joins) | `spark.sql.adaptive.skewJoin.enabled` |
| AQE small partition coalescing | `spark.sql.adaptive.coalescePartitions.enabled` |
| Automatic broadcast threshold | `spark.sql.autoBroadcastJoinThreshold` |
| Executor JVM heap | `spark.executor.memory` |
| Managed off-heap enable/size | `spark.memory.offHeap.enabled`, `spark.memory.offHeap.size` |
| Inspect execution plan | `df.explain('formatted')` or `df.explain('cost')` |
| Release cached DataFrame | `df.unpersist()` |

**Anchor:** **FILES = bytes · SHUFFLE = count · EXECUTOR = heap · AQE = runtime re-optimization.**

## 4. Recall progress (updated after practice)

| Q | Topic | Current level | Specific point to re-test |
|---|---|---:|---|
| 1 | Slow wide transformation | **2** | Skew vs expensive shuffle; read-time pushdown; UDF check |
| 2 | Post-`groupBy` partitioning | **1** | Input vs output size; AQE coalesce; repartition/coalesce/write distinction |
| 3 | Caching and OOM | **2** | Storage tab; `unpersist`; checkpoint trade-off |
| 4 | Z-ORDER | **2** | Selectivity; low-cardinality limitation; partition pruning |
| 5 | High GC time | **2** (guided) | Objects/heap/GC; targeted tuning; delayed cold recall |
| 6 | JSON to Parquet | **2** (guided) | Exact scan-vs-shuffle config names; stragglers; output files |
| 7 | Tungsten | **2** (guided) | Cache-aware ≠ caching; WSCG/operator fusion; delayed cold recall |

**Pending separate revisit:** **Q39 — Spark on Kubernetes in production (Level 1)** → CPU/memory requests and limits; container image; RBAC/namespace; shuffle tracking; node affinity/tolerations; durable checkpoints; compare with YARN.

## 5. How to use this bank (3–5 minutes)

1. Hide the third column and read only the **interview clue**.
2. Say the **keyword sequence out loud** (15–20 sec); then build a **45–60 sec explanation** using the answer pattern.
3. Check the keywords and especially the **avoid this mix-up** column.
4. Retry missed cues later; for Level 2 questions, do a **delayed cold recall** before marking Level 3.

---

### Extension format (add as Q8–Q50 are covered)

`Q# | Hear this clue | Recall 4–8 essential keywords in causal order | Avoid this mix-up`

Keep this document **compact**. Full explanations stay in the mentor's PDF; this file is solely the retrieval/recognition aid.
