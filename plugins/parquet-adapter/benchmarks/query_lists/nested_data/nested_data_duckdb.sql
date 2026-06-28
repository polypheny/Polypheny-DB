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
FROM (
  SELECT list_transform(
    c_orders,
    o -> struct_pack(
      o_orderkey := o.o_orderkey,
      o_orderstatus := o.o_orderstatus,
      o_totalprice := o.o_totalprice,
      o_orderdate := o.o_orderdate,
      o_orderpriority := o.o_orderpriority,
      o_clerk := o.o_clerk
    )
  ) AS c_orders
  FROM nested_customer
) c,
UNNEST(c.c_orders) AS orders(o)
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
FROM nested_customer c,
UNNEST(c.c_orders) AS orders(o),
UNNEST(
  list_transform(
    o.o_lineitems,
    l -> struct_pack(
      l_partkey := l.l_partkey,
      l_suppkey := l.l_suppkey,
      l_linenumber := l.l_linenumber,
      l_quantity := l.l_quantity,
      l_extendedprice := l.l_extendedprice,
      l_discount := l.l_discount,
      l_returnflag := l.l_returnflag,
      l_shipmode := l.l_shipmode
    )
  )
) AS lineitems(l)
WHERE l.l_returnflag = 'R'
  AND l.l_shipmode = 'MAIL'
LIMIT 100000;

-- Q05: Nested lineitem MAX aggregation grouped by return flag.
SELECT
  l.l_returnflag,
  MAX(l.l_extendedprice) AS max_extendedprice
FROM nested_customer c,
UNNEST(c.c_orders) AS orders(o),
UNNEST(
  list_transform(
    o.o_lineitems,
    l -> struct_pack(
      l_extendedprice := l.l_extendedprice,
      l_returnflag := l.l_returnflag
    )
  )
) AS lineitems(l)
GROUP BY l.l_returnflag;
