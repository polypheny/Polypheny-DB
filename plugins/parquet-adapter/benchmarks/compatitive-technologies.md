# Competitive Technologies for Planned Parquet Aggregation Benchmarks

## Purpose

This document describes the technologies selected for a planned comparison of
aggregation queries over Parquet files:

- Polypheny with the Parquet adapter
- DuckDB
- Apache Spark SQL

The goal is to evaluate Polypheny's Parquet adapter against relevant reference
systems. The comparison is intended to show how different analytical
architectures approach the same immutable Parquet dataset and equivalent query
shapes.

The technologies are not identical products:

- Polypheny is the multi-model DBMS and Parquet adapter under evaluation.
- DuckDB is a focused, in-process analytical database reference.
- Apache Spark SQL is a distributed data-processing reference.

Using both DuckDB and Spark gives the research two complementary baselines:
one optimized for efficient single-node analytics and one designed for
large-scale data-lake processing.

## Research Questions

The planned benchmark should help answer the following questions:

1. How competitive is the Polypheny Parquet adapter for analytical aggregation
   queries over external Parquet files?
2. How effectively can each technology use Hive-style folder partitions,
   Parquet metadata, column projection, and filter pushdown?
3. How does Polypheny compare with a specialized single-node analytical engine?
4. How does Polypheny compare with an established data-lake processing engine?
5. Which performance differences are likely to follow from implementation
   architecture rather than from the dataset or query semantics?

The comparison is not intended to be a universal database ranking. It should
not be used to draw conclusions about transactional workloads, concurrent
updates, multi-node scalability, or every feature offered by the selected
systems.

## Comparison Summary

| Characteristic                              | Polypheny Parquet Adapter                                 | DuckDB                                                  | Apache Spark SQL                                                        |
|---------------------------------------------|-----------------------------------------------------------|---------------------------------------------------------|-------------------------------------------------------------------------|
| Planned role                                | System under evaluation                                   | Single-node OLAP reference                              | Distributed data-processing reference                                   |
| General system type                         | Multi-model DBMS with adapter-based external data sources | In-process analytical database                          | Distributed analytics engine                                            |
| Parquet access                              | Direct adapter scan of external Parquet files             | Direct Parquet scan, for example through `read_parquet` | Direct Parquet scan through Spark SQL or DataFrames                     |
| Data import required for planned comparison | No                                                        | No                                                      | No                                                                      |
| Hive-style partition support                | Supported by the Polypheny adapter                        | Supported                                               | Supported                                                               |
| Typical query interface                     | SQL through Polypheny                                     | SQL                                                     | Spark SQL                                                               |
| Main analytical value                       | Evaluates the implemented adapter in its DBMS context     | Provides a focused low-overhead analytical reference    | Provides a mature data-lake and distributed-processing reference        |
| Main comparison caveat                      | Includes broader DBMS and adapter responsibilities        | Embedded architecture has less integration overhead     | Distributed-style execution introduces planning and scheduling overhead |

## Planned Benchmark Scope

The initial benchmark should focus on relational SQL aggregation workloads over
the same immutable Parquet snapshot. Each technology should read the Parquet
files directly without importing them into engine-managed storage.

| Workload category             | Purpose                                              |
|-------------------------------|------------------------------------------------------|
| Full-table counts             | Evaluate metadata use and complete table coverage    |
| Partition-restricted counts   | Evaluate Hive-style folder pruning                   |
| Timestamp and numeric filters | Evaluate filter pushdown and row-group pruning       |
| Column projections            | Evaluate column pruning and row materialization cost |
| Grouped aggregates            | Evaluate `COUNT`, `SUM`, and `AVG` execution         |
| Ordered Top-N aggregates      | Evaluate grouping followed by sorting and limiting   |
| Aggregate joins               | Evaluate joins between compact aggregate results     |

The comparison should keep the following inputs equivalent:

| Controlled input | Planned rule                                                         |
|------------------|----------------------------------------------------------------------|
| Dataset          | Use the same Parquet snapshot for all technologies                   |
| File layout      | Preserve the same folders, files, compression codecs, and row groups |
| Partitioning     | Use the same Hive-style `year=YYYY/month=MM` hierarchy               |
| Query meaning    | Use semantically equivalent SQL for all engines                      |
| Hardware         | Run on the same host or document resource differences explicitly     |
| Caching          | Separate cold-cache and warm-cache experiments where feasible        |
| Results          | Validate row counts and aggregate values before comparing timings    |

## Polypheny Parquet Adapter

### Characteristics

Polypheny is a multi-model DBMS based on a polystore architecture. It supports
relational, document, and graph data models and integrates external data
sources through adapters.

The Parquet adapter is relevant to this research because it exposes Parquet
files as read-only external tables and collections without requiring an import
step. For relational queries, the adapter participates in Polypheny query
planning and can use Parquet-aware execution paths for supported operations.

### Strengths and Weaknesses

| Aspect            | Strengths                                                                                                                                     | Weaknesses or expected costs                                                                                                                                   |
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Data integration  | Makes Parquet files available through the Polypheny catalog and query interface. The same DBMS can integrate other sources and data models.   | Catalog, planning, adapter, and interface layers add responsibilities that a specialized embedded engine does not have.                                        |
| Query model       | Supports SQL while belonging to a broader multi-model architecture.                                                                           | A relational aggregation benchmark cannot demonstrate the complete value of Polypheny's multi-model design.                                                    |
| Parquet execution | Can use adapter-aware scans, partition pruning, projection pushdown, predicate pushdown, and optimized aggregation paths for supported cases. | Optimizations may cover a narrower set of query patterns than the complete SQL language surface. Unsupported patterns may fall back to more general execution. |
| Operational role  | Useful when Parquet data must participate in an existing data-management system without ETL into another store.                               | More infrastructure is involved than in a standalone embedded analytics library.                                                                               |
| Research value    | Directly measures the implementation that the research aims to evaluate and improve.                                                          | Care is needed to separate adapter behavior from broader DBMS overhead.                                                                                        |

### Suitable Use Cases

Polypheny is a strong candidate when Parquet data should be integrated into a
database environment, combined with other sources, or exposed through multiple
data models.

### Less Suitable Use Cases

Polypheny is not necessarily the simplest option for a small analytical script
that only needs to query local Parquet files on one machine. An embedded engine
may require less setup for that narrow use case.

## DuckDB

### Characteristics

DuckDB is an in-process analytical database. It can query one or many Parquet
files directly, infer Hive-style partitions, and push projections and filters
into Parquet scans. DuckDB uses vectorized query execution and is designed for
analytical workloads.

### Strengths and Weaknesses

| Aspect               | Strengths                                                                                             | Weaknesses or expected costs                                                                       |
|----------------------|-------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| Deployment           | Runs in process without a separate database server.                                                   | The embedded model does not represent the operational behavior of a server-based, multi-user DBMS. |
| Analytical execution | Specialized for OLAP queries with vectorized execution and Parquet-aware scans.                       | Its narrow analytical focus does not include Polypheny's broader integration responsibilities.     |
| Parquet support      | Reads multiple Parquet files directly and supports automatic projection and filter pushdown.          | Performance still depends on file count, compression, row-group layout, and metadata quality.      |
| Scale model          | Well suited to efficient single-node analytics.                                                       | It is not the distributed-cluster reference in this research plan.                                 |
| Research value       | Provides a focused reference for the performance achievable by a specialized local analytical engine. | It is not an architecture-equivalent replacement for Polypheny.                                    |

### Suitable Use Cases

DuckDB is well suited to local analytics, interactive data exploration,
embedded applications, and reproducible Parquet experiments on one machine.

### Less Suitable Use Cases

DuckDB is less representative when the target environment requires a
persistent server boundary, heterogeneous data-source integration, multi-model
querying, or distributed execution.

## Apache Spark SQL

### Characteristics

Apache Spark is a unified analytics engine for large-scale data processing.
Spark SQL is its structured-data module. It supports Parquet files, schema
preservation, partition discovery from directory paths, and Parquet filter
pushdown.

Spark is included to represent a mature data-lake processing architecture. A
local experiment can provide an initial comparison, while a future multi-node
experiment could evaluate scaling behavior separately.

### Strengths and Weaknesses

| Aspect          | Strengths                                                                                           | Weaknesses or expected costs                                                                     |
|-----------------|-----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| Scale model     | Designed to distribute work across a cluster and process large datasets.                            | Distributed-style planning and scheduling can add overhead for short queries and small datasets. |
| Parquet support | Mature Parquet integration, schema handling, partition discovery, and configurable filter pushdown. | General-purpose execution can be heavier than a specialized embedded analytical path.            |
| Ecosystem       | Widely used for data-lake analytics, batch processing, and ETL.                                     | Spark is not primarily a low-latency database-serving layer.                                     |
| Execution model | Task-based processing provides an important contrast with single-process analytics.                 | A local run does not demonstrate cluster scaling, network shuffle behavior, or fault tolerance.  |
| Research value  | Provides a recognizable reference for Parquet analytics in a distributed-processing ecosystem.      | Results must be interpreted with awareness of startup and scheduler costs.                       |

### Suitable Use Cases

Spark is appropriate for large data-lake pipelines, batch analytics, ETL, and
workloads that may need to scale across machines.

### Less Suitable Use Cases

Spark is not the simplest choice for low-latency interactive queries over small
local datasets. Its runtime overhead is a trade-off for distributed-processing
capabilities.

## Implementation Proximity

The three technologies are close at the storage-access layer because all can
query Parquet files directly. They differ substantially above that layer.

| Layer                        | Polypheny Parquet Adapter                                        | DuckDB                                                 | Apache Spark SQL                                      |
|------------------------------|------------------------------------------------------------------|--------------------------------------------------------|-------------------------------------------------------|
| Input for planned comparison | Same immutable Parquet snapshot                                  | Same immutable Parquet snapshot                        | Same immutable Parquet snapshot                       |
| Partition layout             | Hive-style `year=YYYY/month=MM` folders                          | Hive-style folders interpreted during Parquet scanning | Hive-style folders discovered by Spark                |
| Storage access               | Direct file reads through the adapter                            | Direct file reads through DuckDB's Parquet scan        | Direct file reads through Spark's Parquet data source |
| Query representation         | SQL planned by Polypheny and translated into adapter execution   | SQL planned and executed by DuckDB                     | SQL planned by Spark SQL and executed as Spark tasks  |
| Execution style              | DBMS planning plus adapter-specific readers and operators        | Vectorized single-node analytical operators            | Partitioned task execution with Spark scheduling      |
| Primary design goal          | Multi-model data management and heterogeneous source integration | Efficient in-process OLAP                              | Cluster-scale data processing                         |

### Relative Closeness

| Comparison           | Where the systems are close                                                                                | Important differences                                                                                            |
|----------------------|------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| Polypheny and DuckDB | Both can directly query local Parquet files and evaluate comparable SQL aggregation shapes.                | DuckDB is embedded and specialized. Polypheny includes DBMS, catalog, planning, and adapter layers.              |
| Polypheny and Spark  | Both can treat partitioned Parquet files as external analytical data and apply partition-aware processing. | Spark is designed around distributed task execution. Polypheny exposes data through a DBMS adapter architecture. |
| DuckDB and Spark     | Both are mature Parquet analytical references.                                                             | DuckDB emphasizes an efficient local process. Spark emphasizes scalable distributed processing.                  |

No selected competitor is an exact implementation match for Polypheny. This is
intentional. DuckDB and Spark provide different reference points around the
Polypheny adapter rather than duplicate baselines.

## Why These Technologies Were Selected

| Selection criterion                                 | Polypheny                | DuckDB                        | Apache Spark SQL               |
|-----------------------------------------------------|--------------------------|-------------------------------|--------------------------------|
| Required to evaluate the target implementation      | Yes                      | No                            | No                             |
| Queries Parquet files directly without import       | Yes                      | Yes                           | Yes                            |
| Supports aggregation-oriented SQL workloads         | Yes                      | Yes                           | Yes                            |
| Supports Hive-style folder partitions               | Yes                      | Yes                           | Yes                            |
| Practical for a reproducible workstation experiment | Yes                      | Yes                           | Yes, in local mode             |
| Adds a distinct architectural perspective           | Multi-model DBMS adapter | Embedded OLAP engine          | Distributed analytics engine   |
| Appropriate role                                    | Target system            | Focused single-node reference | Data-lake processing reference |

The initial set is deliberately limited to three technologies. Every additional
engine increases setup effort, SQL adaptation work, result-validation effort,
and the number of architectural differences that must be explained.

DuckDB and Spark were chosen because they cover two widely relevant analytical
architectures while allowing the same Parquet files and broadly equivalent SQL
queries to be used. Polypheny can then be evaluated in context rather than
against only one type of competitor.


## References

- [Polypheny documentation: Overview](https://docs.polypheny.com/en/latest/getting_started/introduction)
- [Polypheny documentation: Adapter Development](https://docs.polypheny.com/en/latest/devs/adapter-development)
- [DuckDB documentation: Reading and Writing Parquet Files](https://duckdb.org/docs/stable/data/parquet/overview)
- [DuckDB documentation: Execution Format](https://duckdb.org/docs/lts/internals/vector.html)
- [Apache Spark: Spark SQL and DataFrames](https://spark.apache.org/sql/)
- [Apache Spark documentation: Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
