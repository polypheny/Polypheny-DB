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
