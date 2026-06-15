# Nested Data Query Specification

| Query | Operation                                       | Predicate                                                      | Returned data                      | Purpose                                                                |
|-------|-------------------------------------------------|----------------------------------------------------------------|------------------------------------|------------------------------------------------------------------------|
| Q01   | Filtered full customer scan                     | `c_mktsegment = 'BUILDING'` and `c_nationkey BETWEEN 5 AND 15` | Full customer rows or documents    | Measures filtered root-level access with full customer materialization |
| Q02   | Filtered customer projection                    | `c_mktsegment = 'BUILDING'` and `c_acctbal >= 0`               | Explicit customer scalar fields    | Measures filtered projection over root-level customer fields           |
| Q03   | Filtered nested order projection                | `o_orderstatus = 'F'` and `o_totalprice >= 100000`             | Selected order fields              | Measures one-level nested access to order elements                     |
| Q04   | Filtered deeply nested lineitem projection      | `l_returnflag = 'R'` and `l_shipmode = 'MAIL'`                 | Selected lineitem fields           | Measures deeper nested access to lineitem elements                     |
| Q05   | Unfiltered two-level nested lineitem projection | None                                                           | Selected order and lineitem fields | Measures multi-step nested traversal without filter interaction        |

All queries cap the returned result set at `100000` rows or documents. The
Polypheny relational query list intentionally keeps Q03 and Q04 to one
parent-child join, while Q05 is the explicit two-join nested traversal probe.

## Q1 - Filtered full customer scan

Text:

```text
Find customer records from the BUILDING market segment where the nation key is
between 5 and 15, and return every available customer field.
```

SQL:

```text
SELECT *
FROM ncp__nestedcustomer
WHERE c_mktsegment = 'BUILDING'
  AND c_nationkey BETWEEN 5 AND 15
LIMIT 100000;
```

## Q2 - Filtered customer projection

Text:

```text
Find customer records from the BUILDING market segment with a non-negative
account balance, and return only the explicit customer scalar fields.
```

SQL:

```text
SELECT
  c_custkey,
  c_name,
  c_address,
  c_nationkey,
  c_phone,
  c_acctbal,
  c_mktsegment,
  c_comment
FROM ncp__nestedcustomer
WHERE c_mktsegment = 'BUILDING'
  AND c_acctbal >= 0
LIMIT 100000;
```

## Q3 - Filtered nested order projection

Text:

```text
Find nested order elements with final order status and total price at least
100000, and return selected order fields.
```

SQL:

```text
SELECT
  col.__polypheny_row_id AS orders_list_row_id,
  col.__polypheny_parent_row_id AS orders_group_row_id,
  o.o_orderkey,
  o.o_orderstatus,
  o.o_totalprice,
  o.o_orderdate,
  o.o_orderpriority,
  o.o_clerk
FROM ncp__nestedcustomer__c_orders__list col
JOIN ncp__nestedcustomer__c_orders__list__element o
  ON o.__polypheny_parent_row_id = col.__polypheny_row_id
WHERE o.o_orderstatus = 'F'
  AND o.o_totalprice >= 100000
LIMIT 100000;
```

## Q4 - Filtered deeply nested lineitem projection

Text:

```text
Find nested lineitem elements with return flag R and ship mode MAIL, and return
selected lineitem fields.
```

SQL:

```text
SELECT
  olil.__polypheny_row_id AS lineitems_list_row_id,
  olil.__polypheny_parent_row_id AS lineitems_group_row_id,
  l.l_partkey,
  l.l_suppkey,
  l.l_linenumber,
  l.l_quantity,
  l.l_extendedprice,
  l.l_discount,
  l.l_returnflag,
  l.l_shipmode
FROM ncp__nestedcustomer__c_orders__list__element__o_lineitems__list olil
JOIN ncp__nestedcustomer__c_orders__list__element__o_lineitems__list__element l
  ON l.__polypheny_parent_row_id = olil.__polypheny_row_id
WHERE l.l_returnflag = 'R'
  AND l.l_shipmode = 'MAIL'
LIMIT 100000;
```

## Q5 - Unfiltered two-level nested lineitem projection

Text:

```text
Traverse from nested order lineitem groups to lineitem list elements without a
filter, and return selected lineitem fields.
```

SQL:

```text
SELECT
  oli.__polypheny_row_id AS lineitems_group_row_id,
  olil.__polypheny_row_id AS lineitems_list_row_id,
  l.l_partkey,
  l.l_suppkey,
  l.l_linenumber,
  l.l_quantity,
  l.l_extendedprice,
  l.l_discount,
  l.l_returnflag,
  l.l_shipmode
FROM ncp__nestedcustomer__c_orders__list__element__o_lineitems oli
JOIN ncp__nestedcustomer__c_orders__list__element__o_lineitems__list olil
  ON olil.__polypheny_parent_row_id = oli.__polypheny_row_id
JOIN ncp__nestedcustomer__c_orders__list__element__o_lineitems__list__element l
  ON l.__polypheny_parent_row_id = olil.__polypheny_row_id
LIMIT 100000;
```
