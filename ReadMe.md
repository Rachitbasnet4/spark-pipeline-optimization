# Spark Capstone: Fix the Slow Pipeline

An e-commerce nightly revenue job, fixed in two ways: the data is cleaned so the numbers are right, and the job is sped up, with Spark UI evidence for each change.

## Technology & Environment

* **Apache Spark:** 3.4.1
* **PySpark:** DataFrame API and Spark SQL
* **Python**, in a **Jupyter notebook** (`capstone.ipynb`)
* **Execution mode:** `local[*]`
* **Available cores:** 8 (`defaultParallelism = 8`)
* **Input format:** CSV
* **Output format:** CSV
* **Spark UI:** `http://localhost:4040`
* **Not used (per the assignment rules):** pandas, `toPandas()`, Parquet, Delta

## Project structure

```text
spark-pipeline-optimization/
├── capstone.ipynb                     full pipeline, runs top to bottom
├── README.md
├── make_data.py                       generates data
├── .gitignore
├── data/                              input CSVs (not in the repo, see note below)
├── out/
│   ├── orphan_orders/                 orders whose customer does not exist
│   ├── q1_country_revenue/
│   ├── q1_country_revenue_sql/        Q1 again via Spark SQL (same result)
│   ├── q2_top_customers/
│   ├── q3_category_revenue/
│   ├── q4_signup_cohort_avg_order_value/
│   ├── q5_monthly_revenue_2025/
│   └── q6_best_category_by_country/
└── screenshots/
    ├── c1_before.png, c1_after.png
    ├── c2_before.png, c2_after.png
    ├── c3_before.png, c3_after.png
    ├── c4_before.png, c4_after.png
    ├── c4_before_1.png, c4_before_2.png, c4_after_1.png, c4_after_2.png
    ├── c4_cached_storage.png, c4_unpersist_storage.png
    └── c5_before.png, c5_after.png
```

`data/` is not in the repo (about 46 MB). Run `python make_data.py` to generate it, then run `capstone.ipynb`.


## Part A: Data quality accounting

| Step | Rows |
|---|---:|
| Orders at start | 1,020,025 |
| Exact duplicates removed (`dropDuplicates()` on all columns, because the duplicates are exact copies) | 20,025 |
| Bad amounts removed (null, zero, negative) | 15,022 |
| Bad timestamps removed (`"not-a-date"`) | 4,859 |
| Orphan orders set aside in `out/orphan_orders` | 9,892 |
| **Clean orders** | **970,227** |

Check: 1,020,025 - 20,025 - 15,022 - 4,859 - 9,892 = 970,227. Customers: 10,300 down to **10,000** (the row with the latest `signup_date` is kept using `row_number()`).

## Part B: Outputs

The six answers are in `out/` (see the structure above). Q1 was written with the DataFrame API and with `spark.sql`; the two `explain()` plans are identical, because both go through the same Catalyst optimizer. NP leads with 47,198,473.97 revenue from 582,205 orders (60.0% of the 970,227 clean orders).

## Part C: Before and after

C1 to C4 ran with `autoBroadcastJoinThreshold = -1` and AQE off.

| Test | Before | After | Screenshots |
|---|---|---|---|
| C1 Broadcast join | SortMergeJoin. Orders side: 37.4 MiB data size, 18.5 MiB shuffle written. Customers side: 241.4 KiB, 115.0 KiB written. Query 10 s | BroadcastHashJoin with a BroadcastExchange for customers. The orders-side shuffle for the hidden `left_semi` join is still in the plan. Query 12 s | `c1_before.png`, `c1_after.png` |
| C2 Shuffle partitions | 200 tasks. Median task reads 0 B, max 14.8 KiB. Shuffle read 118.7 KiB / 1,600 records. Total task time 0.7 s | 16 tasks. Shuffle read 8.6 KiB / 128 records. Total task time 37 ms | `c2_before.png`, `c2_after.png` |
| C3 AQE | AQE off: 16 tasks, total task time 67 ms | AQE on: 1 task, total task time 3 ms | `c3_before.png`, `c3_after.png` |
| C4 Cache | Plan starts with `Scan csv`. Both aggregations about 2 s | `InMemoryTableScan`. 0.4 s and 0.3 s. Storage tab: 16 partitions, 100% cached, 12.5 MiB. Empty after `unpersist()` | `c4_before_1.png`, `c4_before_2.png`, `c4_after_1.png`, `c4_after_2.png`, `c4_cached_storage.png`, `c4_unpersist_storage.png` |
| C5 UDF vs `when/otherwise` | Python UDF, second run: 3 s | Native expression, second run: 2 s | `c5_before.png`, `c5_after.png` |

**C1.** `clean_orders` is lazy and already contains a `left_semi` join with customers (the orphan filter from Part A), so the plan has two joins. Broadcasting turned my inner join into a BroadcastHashJoin, but the hidden `left_semi` join stayed a SortMergeJoin, so the big orders shuffle remained. That is why the total time did not improve (10 s vs 12 s, which is within laptop noise plus the extra job that builds the broadcast table).

**C2.** I chose 16 shuffle partitions, which is 2x my 8 cores (the class rule is 2 to 4x). The final groupBy moves only about 119 KiB and has 8 keys (one per country), so 200 partitions meant about 192 empty tasks. 16 keeps all cores busy without creating empty tasks.

**C3.** AQE merged the 16 mostly empty shuffle partitions into 1 task, using the real partition sizes it measured at runtime. In this plain groupBy the skew is hidden: Spark pre-adds amounts inside each input partition, so each country sends only a few small records, and the max task read just 1.5 KiB against a median of 0 B, even though NP holds 60% of the rows. AQE could not fix a single hot key: all NP rows must be combined by one task, and AQE only splits skewed partitions in sort-merge joins.

**C4.** Without cache, each aggregation re-read the CSV and redid the filter and join. After `cache()` plus a materializing `count()`, both aggregations read from memory. The C4 screenshots were taken with AQE on (the plans show `AQEShuffleRead`) because it was left on after C3; the cache proof does not depend on it.

**C5.** I used the assignment's thresholds (large above 200, medium above 50). The notebook checks that both versions return identical counts. The UDF sends every row from the JVM to a Python process and back, while `when/otherwise` runs inside Spark's JVM where the optimizer can see it. The SQL tab rounds to whole seconds, so the roughly 1.5x gap is approximate.

## If this ran on a five-node cluster

The driver would no longer share a machine with the executors. For a nightly job I would use cluster deploy mode, so the driver runs inside the cluster and survives my laptop closing (client mode only for interactive work). The input CSVs would have to sit on shared storage (HDFS or S3), not a local path. Shuffles would now cross the network, so shuffle bytes that were cheap on a laptop become a real cost, and broadcasting the small customers table would matter more. I would size `spark.sql.shuffle.partitions` from total cores, for example 5 nodes x 8 cores = 40, so 80 to 160 partitions instead of 16. AQE is on by default in modern Spark, and I would expect it to coalesce small shuffle partitions and switch the customers join to a broadcast on its own once it sees the real table size. It still would not fix the NP hot key in a groupBy, so I would use salting there. My `collect()` calls stay safe only because their results are 3 to 10 rows.

## One honest sentence

I expected broadcasting to make C1 clearly faster, but the lazy `left_semi` join hidden inside `clean_orders` kept the big orders shuffle in the plan, so the run was even 2 s slower (12 s vs 10 s).
