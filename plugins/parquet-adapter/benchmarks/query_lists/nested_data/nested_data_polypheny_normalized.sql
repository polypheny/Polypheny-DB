-- Q01: Filtered full customer rows. Use only root level fields
SELECT *
FROM ncp__nestedcustomer
WHERE c_mktsegment = 'BUILDING'
  AND c_nationkey BETWEEN 5 AND 15
LIMIT 100000;

-- Q02: Filtered explicit customer projection.
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

-- Q03: One-join filtered order element projection.
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

-- Q04: One-join filtered lineitem element projection.
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

-- Q05: Nested lineitem MAX aggregation grouped by return flag.
SELECT
  l.l_returnflag,
  MAX(l.l_extendedprice) AS max_extendedprice
FROM ncp__nestedcustomer__c_orders__list__element__o_lineitems__list olil
JOIN ncp__nestedcustomer__c_orders__list__element__o_lineitems__list__element l
  ON l.__polypheny_parent_row_id = olil.__polypheny_row_id
GROUP BY l.l_returnflag;
