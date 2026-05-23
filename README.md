# Cquirrel-Flink-TPCH-Q9

An incremental streaming query implementation for **TPC-H Q9** based on **Cquirrel (SIGMOD'20)** theory, featuring an end-to-end correctness validation framework on Apache Flink.

## 📖 Project Overview

In modern real-time data analytics, traditional streaming engines often decompose multi-way joins into two-way joins and materialize intermediate states, leading to memory bloat and accumulated latency. The **Cquirrel** theory (SIGMOD'20) addresses this by using **Live Tuples Tracking** and **Delta Enumeration**, proving that acyclic foreign-key multi-way joins can be maintained in amortized $O(\lambda)$ time (degrading to amortized $O(1)$ in common industrial FIFO/sliding window scenarios where $\lambda=1$).

This project implements a distributed incremental streaming processor for **TPC-H Query 9** on Apache Flink based on this theory. It achieves **zero intermediate materialization** for six-table joins and constant-time incremental updates. Furthermore, it provides a rigorous automated validation pipeline to strictly prove the mathematical equivalence between the accumulated incremental deltas and static full-query results.

## ✨ Key Features

- 🚀 **Zero-Materialization Multi-way Join**: Converges the 6-table join into a single Flink `KeyedProcessFunction`, avoiding the memory overhead of materializing intermediate `DataStream`s.
- ⚡ **Constant-Time Incremental Updates**: Utilizes the State Flip paradigm to output only profit changes (`ProfitDelta`), enabling $O(1)$ accumulation in downstream aggregation operators.
- 🌐 **Local Simulated Distribution**: Leverages Flink `KeyedState` and `keyBy` mechanisms to perfectly simulate horizontal sharding and state isolation of a real cluster within a single-machine multi-threaded environment.
- ✅ **End-to-End Correctness Loop**: Provides a complete validation toolchain: `gen_stream` (constructs controllable $\lambda$ streams) → `extract_snapshot` (aligns snapshots) → MySQL executes `Q9.sql` (generates ground truth) → `compare_q9.py` (high-precision Delta accumulation comparison).

## 📂 Repository Structure

```text
├── CquirrelQ9Flink.java        # Core Flink incremental streaming job (Java)
├── gen_stream.py               # Data Generator: Converts TPC-H .tbl files to +/- update streams
├── extract_snapshot_sql.py     # Snapshot Extractor: Syncs Flink input CSVs with MySQL import SQL scripts
├── import_data.sql             # MySQL table creation and static data import scripts
├── Q9.sql                      # Standard TPC-H Q9 SQL query (used for generating Ground Truth)
├── compare_q9.py               # Correctness Validator: Compares Delta accumulation sums with MySQL results
└── README.md                   # Project documentation
```

## 🛠️ Prerequisites

- **Java / Flink**: JDK 1.8+, Apache Flink 1.18.x
- **Python**: Python 3.8+ (Standard library only, no extra `pip install` required)
- **Database**: MySQL 8.0+ (Ensure `local_infile` is enabled)
- **Data Generator**: TPC-H `dbgen` tool

## 🚀 Quick Start (End-to-End Validation Pipeline)

### 1. Generate TPC-H Static Data and Update Streams
Use `dbgen` to generate test data (e.g., SF=0.01) and convert it into a time-series update stream:

```bash
# 1. Generate .tbl files using dbgen (place them in ./tpch_data/)
./dbgen -s 0.01

# 2. Generate FIFO update stream (λ=1)
python gen_stream.py
```
*Output: `input_data_all.csv`*

### 2. Extract Snapshot and Prepare MySQL Baseline
Truncate the data stream at a specified ratio (e.g., 50%) to synchronously generate the Flink input file and the MySQL import script:

```bash
python extract_snapshot_sql.py 0.5 input_data_all.csv snapshot_50pct.sql input_data_50pct.csv
```

Load the snapshot data into MySQL and export the Ground Truth:

```bash
# Import data (ensure 'tpch' database exists in MySQL)
mysql -u root -p tpch_db < import_data.sql
mysql -u root -p tpch_db < snapshot_50pct.sql

# Execute Q9 query and export baseline results
mysql -u root -p tpch_db -e "source Q9.sql" > q9_mysql.csv
```

### 3. Run Flink Incremental Streaming Job
Compile `CquirrelQ9Flink.java` and submit it to the Flink cluster (or run the Main method directly in your IDE):

```bash
# Pass the truncated CSV file and output path
flink run -c CquirrelQ9Flink cquirrel-q9.jar input_data_50pct.csv q9_deltas_flink.csv
```
*Output: `q9_deltas_flink.csv` (contains `+/-` profit deltas)*

### 4. Validate Correctness
Run the comparison script to verify if the Flink incremental accumulation strictly matches the MySQL static query result:

```bash
python compare_q9.py q9_deltas_flink.csv q9_mysql.csv
```

**Expected Output:**
```text
===============================================================================================
Q9 Comparison Report
===============================================================================================
Rounding Precision (PRECISION): 0.01
Error Tolerance (TOLERANCE): 0.02
MySQL Final Rows:     146
Stream Aggregated Rows:   146
Exact Matches:        146
...
√ Validation Passed! Q9 incremental stream accumulation results are fully consistent with MySQL full query.
===============================================================================================
```

## 🧠 Core Algorithm Design

1. **Foreign-Key DAG Modeling**: The 6-table join in Q9 forms a tree-shaped Foreign-Key Directed Acyclic Graph (DAG), with `lineitem` as the leaf node.
2. **Live Tuples Tracking**: Maintains tuples capable of participating in the full join in a bottom-up manner. Dimension table updates trigger cascaded re-evaluations of fact tables via $O(1)$ reverse indexes.
3. **Delta Enumeration**: Only emits a `ProfitDelta` when the active state of a `lineitem` flips (`null ↔ valid`), completely eliminating the materialization of intermediate views.

## 📚 References

- Wang, Q., & Yi, K. (2020). **Maintaining Acyclic Foreign-Key Joins under Updates**. *SIGMOD 2020*.
- Wang, Q., et al. (2021). **Cquirrel: Continuous Query Processing over Acyclic Relational Schemas**. *PVLDB 2021*.
- TPC-H Benchmark Specification: [http://www.tpc.org/tpch/](http://www.tpc.org/tpch/)

## 📄 License

This project is open-sourced under the MIT License. If you use this code or experimental design, please cite the references above and acknowledge the source.