# Polypheny Parquet Adapter Data Configuration for Benchmarks

## Access Model Comparison

```text
location on disk:
C:\PolyData\tlc_partitioned

url:
file:///C:/PolyData/tlc_partitioned

relational adapter name:
tlcp

flat mode

document adapter name:
tlcpd

```

## Nested Data

```text
location on disk:
C:\PolyData\nested_customer\nestedcustomer.parquet

url:
file:///C:/PolyData/nested_customer/nestedcustomer.parquet

relational adapter name:
ncp

adapter normalized mode

document adapter name:
ncpd
```

## Aggregation

same relational adapter as Access Model Comparison

```text
location on disk:
C:\PolyData\tlc_partitioned

url:
file:///C:/PolyData/tlc_partitioned

relational adapter name:
tlcp


document adapter name:
tlcpd

```


## Partitioning

### Repartitioned data, flat

```text
location on disk:
C:\PolyData\tlc_repartitioned

url:
file:///C:/PolyData/tlc_repartitioned

relational adapter name:
tlcr

```

### Unpartitioned data, flat

```text
location on disk:
C:\PolyData\tlc_unpartitioned

url:
file:///C:/PolyData/tlc_unpartitioned

relational adapter name:
tlcu

```
