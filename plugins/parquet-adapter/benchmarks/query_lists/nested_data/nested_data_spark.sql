-- Q01: Filtered full customer rows.
SELECT *
FROM nested_customer
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
FROM nested_customer
WHERE c_mktsegment = 'BUILDING'
  AND c_acctbal >= 0
LIMIT 100000;

-- Q03: Filtered order projection.
SELECT
  o.o_orderkey,
  o.o_orderstatus,
  o.o_totalprice,
  o.o_orderdate,
  o.o_orderpriority,
  o.o_clerk
FROM nested_customer c
LATERAL VIEW explode(c_orders) orders_view AS o
WHERE o.o_orderstatus = 'F'
  AND o.o_totalprice >= 100000
LIMIT 100000;

-- Q04: Filtered lineitem projection.
SELECT
  l.l_partkey,
  l.l_suppkey,
  l.l_linenumber,
  l.l_quantity,
  l.l_extendedprice,
  l.l_discount,
  l.l_returnflag,
  l.l_shipmode
FROM nested_customer c
LATERAL VIEW explode(c_orders) orders_view AS o
LATERAL VIEW explode(o.o_lineitems) lineitems_view AS l
WHERE l.l_returnflag = 'R'
  AND l.l_shipmode = 'MAIL'
LIMIT 100000;

-- Q05: Unfiltered two-level nested lineitem projection.
SELECT
  o.o_orderkey,
  o.o_orderstatus,
  l.l_partkey,
  l.l_suppkey,
  l.l_linenumber,
  l.l_quantity,
  l.l_extendedprice,
  l.l_discount,
  l.l_returnflag,
  l.l_shipmode
FROM nested_customer c
LATERAL VIEW explode(c_orders) orders_view AS o
LATERAL VIEW explode(o.o_lineitems) lineitems_view AS l
LIMIT 100000;
