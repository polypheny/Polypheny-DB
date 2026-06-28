# Aggregation Correctness Summary

Reference system: `DuckDB`.
Numeric values are compared with absolute tolerance `0.000001` and relative tolerance `0.000001`.

## Source Files

| System | Values file |
| --- | --- |
| Polypheny Relational | `plugins\\parquet-adapter\\benchmarks\\results\\aggregation\\aggregation_polypheny_values.jsonl` |
| Polypheny Document MQL | `plugins\\parquet-adapter\\benchmarks\\results\\aggregation\\aggregation_polypheny_mql_values.jsonl` |
| DuckDB | `plugins\\parquet-adapter\\benchmarks\\results\\aggregation\\aggregation_duckdb_values.jsonl` |
| Apache Spark | `plugins\\parquet-adapter\\benchmarks\\results\\aggregation\\aggregation_spark_values.jsonl` |

## Comparison

| Query | Polypheny Relational | Polypheny Document MQL | Apache Spark |
| --- | --- | --- | --- |
| Q01 | ok | ok | ok |
| Q02 | ok | ok | ok |
| Q03 | ok | ok | ok |
| Q04 | ok | ok | ok |
| Q05 | ok | differs: columns differ | ok |
| Q06 | ok | ok | ok |
| Q07 | ok | ok | ok |
| Q08 | ok | ok | ok |
| Q09 | ok | differs: columns differ | ok |
| Q10 | ok | differs: columns differ | ok |

## Compared Values

The following tables align captured result values by query.
Each table includes the reference system so that mismatches can be inspected directly.

### Q01

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 98276264 | 98276264 | 98276264 | 98276264 |

### Q02

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 3675411 | 3675411 | 3675411 | 3675411 |

### Q03

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 127002 | 127002 | 127002 | 127002 |

### Q04

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 6438540 | 6438540 | 6438540 | 6438540 |

### Q05

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| year=2020 / trips | 24649092 |  | 24649092 | 24649092 |
| year=2020 / gross_amount | 454079334.16108584 |  | 4.540793341610861E8 | 454079334.14322543 |
| year=2020 / min_distance | -30.62 |  | -30.62 | -30.62 |
| year=2020 / max_distance | 350914.89 |  | 350914.89 | 350914.89 |
| year=2021 / trips | 30904308 |  | 30904308 | 30904308 |
| year=2021 / gross_amount | 608700670.9132242 |  | 6.087006709132242E8 | 608700670.9059402 |
| year=2021 / min_distance | 0.0 |  | 0.0 | 0.0 |
| year=2021 / max_distance | 351613.36 |  | 351613.36 | 351613.36 |
| year=2022 / trips | 39656098 |  | 39656098 | 39656098 |
| year=2022 / gross_amount | 859397945.173023 |  | 8.593979451730231E8 | 859397945.1733704 |
| year=2022 / min_distance | 0.0 |  | 0.0 | 0.0 |
| year=2022 / max_distance | 389678.46 |  | 389678.46 | 389678.46 |
| year=2023 / trips | 3066766 |  | 3066766 | 3066766 |
| year=2023 / gross_amount | 82865192.2197824 |  | 8.28651922197824E7 | 82865192.2197824 |
| year=2023 / min_distance | 0.0 |  | 0.0 | 0.0 |
| year=2023 / max_distance | 258928.15 |  | 258928.15 | 258928.15 |
| _id=2020 / max_distance |  | 350914.89 |  |  |
| _id=2020 / min_distance |  | -30.62 |  |  |
| _id=2020 / gross_amount |  | 454079334.16108584 |  |  |
| _id=2021 / max_distance |  | 351613.36 |  |  |
| _id=2021 / min_distance |  | 0.0 |  |  |
| _id=2021 / gross_amount |  | 608700670.9132242 |  |  |
| _id=2022 / max_distance |  | 389678.46 |  |  |
| _id=2022 / min_distance |  | 0.0 |  |  |
| _id=2022 / gross_amount |  | 859397945.173023 |  |  |
| _id=2023 / max_distance |  | 258928.15 |  |  |
| _id=2023 / min_distance |  | 0.0 |  |  |
| _id=2023 / gross_amount |  | 82865192.2197824 |  |  |

### Q06

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 548801637 | 548801637 | 548801637 | 548801637 |

### Q07

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 19306090 | 19306090 | 19306090 | 19306090 |

### Q08

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| row_count | 42659531 | 42659531 | 42659531 | 42659531 |

### Q09

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| year=2020 / trips | 143309871 |  | 143309871 | 143309871 |
| year=2020 / passenger_fare | 2527548383.5690694 |  | 2.527548383569069E9 | 2527548383.5680666 |
| year=2020 / driver_pay | 2036707828.5292335 |  | 2.0367078285292332E9 | 2036707828.5286038 |
| year=2020 / min_miles | 0.0 |  | 0.0 | 0.0 |
| year=2020 / max_miles | 1310.51 |  | 1310.51 | 1310.51 |
| year=2021 / trips | 174596652 |  | 174596652 | 174596652 |
| year=2021 / passenger_fare | 3839172144.575017 |  | 3.839172144575017E9 | 3839172144.575018 |
| year=2021 / driver_pay | 3085990462.630042 |  | 3.085990462630042E9 | 3085990462.6300426 |
| year=2021 / min_miles | 0.0 |  | 0.0 | 0.0 |
| year=2021 / max_miles | 738.95 |  | 738.95 | 738.95 |
| year=2022 / trips | 212416083 |  | 212416083 | 212416083 |
| year=2022 / passenger_fare | 5029617856.472827 |  | 5.029617856472826E9 | 5029617856.472827 |
| year=2022 / driver_pay | 3966957717.7361007 |  | 3.9669577177361016E9 | 3966957717.736101 |
| year=2022 / min_miles | 0.0 |  | 0.0 | 0.0 |
| year=2022 / max_miles | 634.32 |  | 634.32 | 634.32 |
| year=2023 / trips | 18479031 |  | 18479031 | 18479031 |
| year=2023 / passenger_fare | 398327570.2230424 |  | 3.983275702230424E8 | 398327570.2230424 |
| year=2023 / driver_pay | 310164377.32070553 |  | 3.1016437732070553E8 | 310164377.32070553 |
| year=2023 / min_miles | 0.0 |  | 0.0 | 0.0 |
| year=2023 / max_miles | 407.563 |  | 407.563 | 407.563 |
| _id=2020 / driver_pay |  | 2036707828.5292335 |  |  |
| _id=2020 / passenger_fare |  | 2527548383.5690694 |  |  |
| _id=2020 / max_miles |  | 1310.51 |  |  |
| _id=2020 / min_miles |  | 0.0 |  |  |
| _id=2021 / driver_pay |  | 3085990462.630042 |  |  |
| _id=2021 / passenger_fare |  | 3839172144.575017 |  |  |
| _id=2021 / max_miles |  | 738.95 |  |  |
| _id=2021 / min_miles |  | 0.0 |  |  |
| _id=2022 / driver_pay |  | 3966957717.7361007 |  |  |
| _id=2022 / passenger_fare |  | 5029617856.472827 |  |  |
| _id=2022 / max_miles |  | 634.32 |  |  |
| _id=2022 / min_miles |  | 0.0 |  |  |
| _id=2023 / driver_pay |  | 310164377.32070553 |  |  |
| _id=2023 / passenger_fare |  | 398327570.2230424 |  |  |
| _id=2023 / max_miles |  | 407.563 |  |  |
| _id=2023 / min_miles |  | 0.0 |  |  |

### Q10

| Result | Polypheny Relational | Polypheny Document MQL | DuckDB | Apache Spark |
| --- | --- | --- | --- | --- |
| shared_request_flag=N / trips | 210564721 |  | 210564721 | 210564721 |
| shared_request_flag=Y / trips | 1851362 |  | 1851362 | 1851362 |
| _id |  | N |  |  |
| _id #2 |  | Y |  |  |
