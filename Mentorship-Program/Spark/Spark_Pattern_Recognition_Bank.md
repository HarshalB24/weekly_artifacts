# Apache Spark — Pattern Recognition Bank

**Source:** Mentor's *Spark Top 50 Interview Questions* (Q1–Q11 reviewed); technical clarifications marked below.  
**Purpose:** Recognize the interview clue → retrieve the right Spark mechanisms → reconstruct your own answer. **This is not a second answer book.**  
**Progress:** Q1–Q11 reviewed (11/50); Q12 is next. All covered questions still need delayed independent recall to qualify for Level 3.

## Catch-up sprint — finish first-pass review by Wednesday, 28 Oct 2026

**Deadline:** Before Thursday, 29 Oct 2026. **Current status:** Q1–Q11 reviewed (not all mastered); Q39 previously attempted, Level 1. **Next cold-recall question:** Q12 — YARN/Kubernetes executor OOM kills.

| Day | New/target questions | Main focus | Review requirement |
|---|---|---|---|
| Fri 23 / early Sat 24 Oct | **Q8–Q11 — done** | AQE, unified memory, spill, off-heap | Four-question recognition batch consolidated below |
| Sat 24 Oct | **Q12–Q23 (12)** | Container OOM, executor memory, GC, joins/skew, start streaming | Short delayed cold-recall checks for Q2/Q5/Q8–Q11 |
| Sun 25 Oct | **Q24–Q37 (14)** | Streaming, Delta/lakehouse and storage | Revisit missed Saturday cues |
| Mon 26 Oct | **Q38–Q42 (5)** | Cluster management, including Q39 Kubernetes | Revisit Level 0–1 topics |
| Tue 27 Oct | **Q43–Q45 (3)** | Production debugging and advanced APIs; JPMorgan priority | 10-minute mixed recall |
| Wed 28 Oct | **Q46–Q50 (5)** | Advanced APIs/architecture and mixed review | Mixed mock across all 50; identify weak spots |

**For each question:** Attempt without notes (60–90 sec) → compare with mentor answer → explain meaningful gaps → derive 3–6 recognition keywords → retry weak points. Level 3 requires accurate *independent* recall, followed by a delayed check. A first pass through 50 is not the same as mastering all 50.

**Daily fast review:** 10–15 minutes of previously missed questions; priority Level 0/1, then Level 2. Update recognition-bank cues **once per completed question batch**, not one answer at a time.

**Protect other study time:** SQL 1h/day (15m mistakes + 45m questions), Python 1h/day, Spark/Data Modelling mixed revision ~1.5h/day, and JPMorgan interview preparation 1–2h on Sat/Sun (at least 1h Mon/Tue if confirmed). New Spark questions need separate focused blocks.

---

## 1. Fast recognition bank — Q1–Q11

| Q | Hear this interview clue | Instantly recall these keywords | Avoid this mix-up |
|---|---|---|---|
| **Q1** | **10 TB; 80% runtime in one wide transformation** | **Spark UI** → shuffle read/write → spill → uneven task times / **skew?** → partition count → `.explain()` (BHJ/SMJ) → AQE/skew join → predicate pushdown → built-ins instead of UDFs → remeasure | One slow stage does **not** prove skew; check task distribution. Predicate pushdown concerns **reading**, not writing Parquet. |
| **Q2** | **500M rows; `groupBy().agg()`; only 200 partitions** | **Shuffle partition count** → inspect actual *aggregated* partition sizes → `spark.sql.shuffle.partitions` → AQE *coalesces small* partitions → `repartition(n)` vs `coalesce(n)` → `write.partitionBy()` if relevant | 500M **input** rows do not mean 500M output rows. Memory doesn't cause the configured 200 partition count. |
| **Q3** | **Many `.cache()` calls; executor OOM** | **Reuse** > once → right storage level → `unpersist()` → **Storage tab** → `clearCache()` (test cleanup) → checkpoint if breaking long lineage is appropriate | Caching after a wide transformation isn't inherently wrong; unnecessary retained caches cause pressure. |
| **Q4** | **Delta query slow even after Z-ORDER** | **Min/max statistics → data skipping** → selective filter on Z-ordered column → not already clustered → low cardinality/small table limitations → **Liquid Clustering / partition pruning / Bloom filters** (when appropriate) | Z-ordering doesn't guarantee faster scans; filter selectivity and file-skipping opportunities matter. |
| **Q5** | **Executor GC > 30% of task time** | **Objects** allocated/retained → JVM heap pressure → **Executors tab** GC metrics → DataFrame/Tungsten → Kryo (relevant workloads) → executor heap/off-heap → G1GC → remeasure | Don't jump straight to AQE or partition count; first investigate object allocation and heap pressure. |
| **Q6** | **Read 1 TB JSON → transforms → write Parquet** | **R-S-E-W:** **Read** file splits/parallelism → **Shuffle** partitions/AQE/broadcast small lookups → **Executors** CPU/heap/local shuffle I/O/speculation → **Write** Parquet Snappy/ZSTD/file layout → validate | `spark.sql.files.maxPartitionBytes` = input **bytes**; `spark.sql.shuffle.partitions` = shuffle **count**. JSON splittability varies by encoding/compression. |
| **Q7** | **Tungsten vs RDD; why faster?** | **B-C-C:** **Binary** compact representation → CPU **Cache-aware** algorithms → whole-stage **Codegen** → less JVM object/operator overhead; limited with direct RDD APIs / Python or unsupported UDF paths | CPU cache locality ≠ `.cache()` persistence. Tungsten isn't always off-heap or columnar. |
| **Q8** | **Adaptive Query Execution: 3 runtime benefits** | **Runtime stats → C-S-J:** **Coalesce** small shuffle partitions → split **Skewed** join partitions → change **Join** strategy (e.g., SMJ→BHJ after filter) → new plan after materialized shuffle stages | AQE based on runtime stats; not “Spark 3.14”; coalescing ≠ `repartition()`, skew splitting is for supported joins. |
| **Q9** | **Unified memory: 60/40; execution vs storage under pressure** | **300 → 60/40 → flexible 50/50 → evict / spill:** ~300 MiB reserved heap → Spark/user fraction of usable heap → execution ↔ storage sharing, storage protected region → cached blocks can be evicted; execution working sets may spill | Reserved heap ≠ executor memory overhead; storage cannot evict active execution. Fixed 50/50 is incorrect. |
| **Q10** | **“Spill to disk” warnings; slow sorting/aggregation** | **M-D-I-F:** **Memory**-intensive sort/aggregate → intermediate data written to **Disk** → **Inspect** Spark UI *Spill (Memory)/(Disk)*, skew, task times → **Fix** oversized partitions (`shuffle.partitions`), memory, pre-aggregation (`reduceByKey` vs `groupByKey`), unnecessary shuffles | **Execution spill** ≠ writing cached blocks under `MEMORY_AND_DISK`; “3–5× longer” is an illustrative impact, not guaranteed; off-heap doesn't automatically stop spills. |
| **Q11** | **Use off-heap? Benefits/risks, configuration** | **O-G-R-B:** **Outside** JVM heap → less **GC** for managed data; consider high GC on large executors/caching workloads → **Risks** native OOM, leaks, tricky debugging → **Budget** `spark.memory.offHeap.enabled`, `spark.memory.offHeap.size`, plus sufficient `spark.executor.memoryOverhead` | Off-heap ≠ free extra memory; it still counts toward container limit. Exact config uses capital **H** in `offHeap`. |

**Fast answer pattern for scenario questions:** **Clue → Spark internal mechanism → Diagnose/verify → Targeted fix → Validate/trade-off.**

## 2. How the eleven questions connect

```text
              SOURCE / FILE LAYOUT
        Q6 read splits / Q4 Delta data skipping
                       |
              TRANSFORM / SHUFFLE
        Q1 slow stage / Q2 partitions
        Q8 AQE = runtime coalesce/skew/join
                       |
           MEMORY + EXECUTION ENGINE
  Q3 cache pressure <---> Q9 shared execution/storage
  Q5 high JVM GC    <---> Q7 Tungsten binary/codegen
                       |
         TWO RESPONSES TO MEMORY PRESSURE
  Q10 spill intermediate working data to disk
  Q9 evict cached blocks (or disk-persist if configured)
  Q11 consider managed off-heap to reduce GC, budget overhead
                       |
                WRITE + VALIDATE
            Q6 Parquet output + Spark UI
```

**Symptom recognition:**

- **Some tasks take 25 min; most take 2 min** → suspect **skew/stragglers**, validate with per-task shuffle size, duration and spill (Q1).
- **All aggregation tasks are huge; partition count is fixed at 200** → inspect **actual shuffle sizes** and tune **partitioning** (Q2).
- **Storage tab keeps filling after repeated use** → check **cached datasets**, reuse, and unpersist (Q3).
- **Filtered Delta scans still read most files** → check **data-skipping statistics and filter selectivity** (Q4).
- **GC time dominates execution** → inspect **JVM objects, heap pressure, and executor GC** (Q5).
- **End-to-end JSON-to-Parquet tuning** → **R → S → E → W** (Q6).
- **Asked why DataFrame execution may outperform object-heavy RDD execution** → **Tungsten B-C-C** (Q7).
- **Join or shuffle strategy changes while job runs** → **AQE runtime C-S-J** (Q8).
- **Execution can reclaim cache memory; cached data evicted; execution spills** → **Unified Memory Model** (Q9).
- **Stage has large spill (memory)/(disk) values** → investigate **sort/hash working set and partition sizing** (Q10), distinguish from persisted cached blocks.
- **Executor GC is costly; off-heap is proposed** → **O-G-R-B** plus container memory accounting (Q11).

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
| Executor container memory overhead | `spark.executor.memoryOverhead` |
| Spark-managed memory fraction / protected storage region | `spark.memory.fraction`, `spark.memory.storageFraction` |
| Managed off-heap enable/size | `spark.memory.offHeap.enabled`, `spark.memory.offHeap.size` |
| Inspect execution plan | `df.explain('formatted')` or `df.explain('cost')` |
| Release cached DataFrame | `df.unpersist()` |

**Anchor:** **FILES = bytes · SHUFFLE = count · EXECUTOR = JVM heap · OVERHEAD = non-heap container allowance · OFFHEAP = memory outside JVM heap · AQE = runtime re-optimization.**

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
| 8 | AQE | **2** (independent) | Three realistic examples; materialized shuffle stage; Spark 3.0 not “3.14” |
| 9 | Unified memory | **2** (guided retry) | Heap reserved vs container overhead; storage eviction vs execution spill |
| 10 | Spill to disk | **2** (independent) | Spark UI metrics; sort/hash mechanism; pre-aggregation and shuffle avoidance |
| 11 | Off-heap memory | **2** (guided from initial **0**) | Exact camel case `offHeap`; native memory risks; container budget; delayed cold recall |

**Pending separate revisit:** **Q39 — Spark on Kubernetes in production (Level 1)** → CPU/memory requests and limits; container image; RBAC/namespace; shuffle tracking; node affinity/tolerations; durable checkpoints; compare with YARN.

## 5. How to use this bank (3–5 minutes)

1. Hide the third column and read only the **interview clue**.
2. Say the **keyword sequence out loud** (15–20 sec); then build a **45–60 sec explanation** using the answer pattern.
3. Check the keywords and especially the **avoid this mix-up** column.
4. Retry missed cues later; for Level 2 questions, do a **delayed cold recall** before marking Level 3.

---

### Extension format (add as Q12–Q50 are covered)

`Q# | Hear this clue | Recall 4–8 essential keywords in causal order | Avoid this mix-up`

Keep this document **compact**. Full explanations stay in the mentor's PDF; this file is solely the retrieval/recognition aid.
