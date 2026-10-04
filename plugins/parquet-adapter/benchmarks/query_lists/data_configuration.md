# Polypheny Parquet Adapter Data Configuration for Benchmarks

## Access Model Comparison

```text
Data location on disk:
C:\PolyData\tlc_partitioned
```

#### Relational Flat adapter:

```text
name:
tlcp

flat mode

url:
file:///C:/PolyData/tlc_partitioned

```

#### Relational Normalized Adapter:

```text
name:
tlcpn

normalized mode

url:
file:///C:/PolyData/tlc_partitioned

```

#### Document Adapter:

```text

name:
tlcpd

url:
file:///C:/PolyData/tlc_partitioned

```

## Nested Data

```text
Data location on disk:
C:\PolyData\nested_customer\nestedcustomer.parquet
```

#### Relational Normalized Adapter

```text
name:
ncp

normalized mode

url:
file:///C:/PolyData/nested_customer/nestedcustomer.parquet
```
#### Document Adapter

```text
name:
ncpd

url:
file:///C:/PolyData/nested_customer/nestedcustomer.parquet
```

## Aggregation
Same adapters as Access Model Comparison

```text
Data location on disk:
C:\PolyData\tlc_partitioned
```

#### Relational Adapter

```text
name:
tlcp

flat mode

url:
file:///C:/PolyData/tlc_partitioned
```

#### Document Adapter

```text
name:
tlcpd

url:
file:///C:/PolyData/tlc_partitioned

```


## Partitioning

```text
Data location on disk:

C:\PolyData\tlc_repartitioned
C:\PolyData\tlc_unpartitioned
```

### Relational Adapter, Repartitioned data, flat

```text
name:
tlcr

flat

url:
file:///C:/PolyData/tlc_repartitioned

```

### Relational Adapter, Unpartitioned data, flat

```text
name:
tlcu

flat

url:
file:///C:/PolyData/tlc_unpartitioned

```
