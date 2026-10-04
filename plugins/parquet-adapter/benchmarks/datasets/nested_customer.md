# Nested Customer Dataset

## Summary

`nestedcustomer.parquet` is a nested Parquet representation of TPC-H scale factor 10 customer data. It keeps each customer as one root row and embeds the customer's orders and each order's line items as arrays of structs:

```text
customer
  c_orders: array<struct<order fields,
    o_lineitems: array<struct<lineitem fields>>>>
```

This makes the dataset a good benchmark candidate for nested-data support because it combines realistic TPC-H cardinalities with repeated nested structures that must be projected, traversed, unnested, or filtered.

## Source And Lineage

The dataset comes from the public `alicerey/nested-parquet` GitHub repository:

```text
https://github.com/alicerey/nested-parquet/tree/main/nested-tpch-sf10
```

That repository is the supplemental material for the paper "Nested Parquet Is Flat, Why Not Use It? How To Scan Nested Data With On-the-Fly Key Generation and Joins" by Alice Rey, Maximilian Rieger, and Thomas Neumann. The repository README describes it as supplemental material for the evaluation and states that the benchmark datasets are hosted in a Google Drive folder.

The local ZIP archive was downloaded from that Google Drive dataset link. The concrete Drive file URL was recovered from the Windows `Zone.Identifier` stream of `C:\PolyData\nested-parquet-datasets.zip`:

```text
https://drive.google.com/file/d/1UqwFbuywMErdgau1_JWN8QwzjMwpXle4/view
```

The archive contains the same benchmark groups visible in the GitHub repository: `dblp`, `nested-tpch`, `nested-tpch-sf10`, `synthetic`, and `xmark`. The `nested-tpch-sf10` directory contains system-specific query subdirectories in the repository and the corresponding Parquet data files in the downloaded archive.

The underlying business data model is TPC-H, the Transaction Processing Performance Council's decision-support benchmark. In the paper's evaluation section, the authors describe the scale factor 10 TPC-H nested dataset as nesting line items into orders and orders into customers. The paper's dataset table reports the same scale and shape observed locally: approximately 2.6 GB, 1.5M root tuples, and 60M tuples at the maximum nesting level.

This local Parquet copy was written by Spark: the Parquet footer reports Spark 4.0.0 metadata and `parquet-mr version 1.13.1`.

Useful references:

- GitHub repository: https://github.com/alicerey/nested-parquet
- `nested-tpch-sf10` repository directory: https://github.com/alicerey/nested-parquet/tree/main/nested-tpch-sf10
- Dataset Google Drive file: https://drive.google.com/file/d/1UqwFbuywMErdgau1_JWN8QwzjMwpXle4/view
- Paper PDF: https://db.in.tum.de/~rey/papers/nestedparquet_rey.pdf
- Paper DOI: https://doi.org/10.1145/3725329
- TPC-H homepage: https://www.tpc.org/tpch/

## Local Files

Primary nested file:

```text
C:\PolyData\nested-parquet-datasets\nested-parquet-datasets\nested-tpch-sf10\nestedcustomer.parquet
```

Normalized comparison files:

```text
C:\PolyData\nested-parquet-datasets\nested-parquet-datasets\nested-tpch-sf10\normalized\nestedcustomer_0.parquet
C:\PolyData\nested-parquet-datasets\nested-parquet-datasets\nested-tpch-sf10\normalized\nestedcustomer_1.parquet
C:\PolyData\nested-parquet-datasets\nested-parquet-datasets\nested-tpch-sf10\normalized\nestedcustomer_2.parquet
```

There is also a copied standalone primary file:

```text
C:\PolyData\nested_customer\nestedcustomer.parquet
```

## Measured Metadata

| File                                  |       Size |       Rows | Notes                                |
|---------------------------------------|-----------:|-----------:|--------------------------------------|
| `nestedcustomer.parquet`              |   2.63 GiB |  1,500,000 | Nested customer table; 22 row groups |
| `normalized/nestedcustomer_0.parquet` | 123.87 MiB |  1,500,000 | Customer-level normalized table      |
| `normalized/nestedcustomer_1.parquet` | 558.62 MiB | 15,500,018 | Order-level normalized table         |
| `normalized/nestedcustomer_2.parquet` |   2.26 GiB | 60,486,070 | Lineitem-level normalized table      |

Parquet footer metadata for the nested file:

- Created by: `parquet-mr version 1.13.1`
- Spark metadata: `org.apache.spark.version = 4.0.0`
- Root rows: `1,500,000`
- Row groups: `22`
- Top-level fields: `9`
- Primitive leaf fields: `31`
- Maximum nested schema depth: `7`
- Repeated/list fields: `2`

## Schema Shape

Top-level fields:

```text
c_custkey: integer
c_name: string
c_address: string
c_nationkey: integer
c_phone: string
c_acctbal: decimal(12,2)
c_mktsegment: string
c_comment: string
c_orders: array<struct<
  o_orderkey: integer,
  o_orderstatus: string,
  o_totalprice: decimal(12,2),
  o_orderdate: date,
  o_orderpriority: string,
  o_clerk: string,
  o_shippriority: integer,
  o_comment: string,
  o_lineitems: array<struct<
    l_partkey: integer,
    l_suppkey: integer,
    l_linenumber: integer,
    l_quantity: decimal(12,2),
    l_extendedprice: decimal(12,2),
    l_discount: decimal(12,2),
    l_tax: decimal(12,2),
    l_returnflag: string,
    l_linestatus: string,
    l_shipdate: date,
    l_commitdate: date,
    l_receiptdate: date,
    l_shipinstruct: string,
    l_shipmode: string,
    l_comment: string
  >>
>>
```

```text
Customer
  Order #123
    Lineitem 1: part A, quantity 2, price 50
    Lineitem 2: part B, quantity 1, price 20
    Lineitem 3: part C, quantity 5, price 10
```

```text
customer
  c_orders
    order
      o_lineitems
        lineitem
```

## Benchmark Use

Recommended benchmark role:

- Primary nested-data benchmark for Parquet scans, projection, and nested-field access.
- Nested unnest/flatten benchmark for `customer -> orders -> lineitems`.
- Comparison benchmark against the normalized files to measure the cost or benefit of nested storage relative to relational-style tables.

Representative query patterns:

- Scan only customer scalar fields to test projection pruning around a large nested column.
- Project selected order fields from `c_orders`.
- Unnest `c_orders` and aggregate order counts per customer.
- Unnest both `c_orders` and `o_lineitems` and aggregate lineitem metrics by customer, order, date, status, or return flag.
- Filter on nested lineitem fields such as ship date, return flag, or discount.

Limitations:

- This is a derived nested layout based on TPC-H, not an official TPC-H benchmark result package.
- GitHub is the public landing page and the data itself is hosted externally on Google Drive.
- The dataset has arrays and structs, but no map fields.
