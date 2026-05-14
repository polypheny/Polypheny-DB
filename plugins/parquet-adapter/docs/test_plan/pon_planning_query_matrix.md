# PON Planning Query Matrix

This checklist is for exercising different physical-planning shapes over the normalized `pon_` Parquet tables.

The queries assume these table relationships:

- `pon__customers`, `pon__orders`, and `pon__products` are root-level tables.
- `pon__orders__items` is a direct child of `pon__orders`.
- `pon__orders__shipping_address` is a direct child of `pon__orders`.
- `pon__orders__items__discounts` is a direct child of `pon__orders__items`.
- Supported adapter joins use generated structural keys: child `__polypheny_parent_row_id` to parent `__polypheny_row_id`.

Plan terms below use the short PolyAlg names:

- `P_SCAN`: Parquet scan.
- `PE_CALC`: converter from Parquet convention to enumerable convention.
- `P_JOIN`: adapter-level Parquet parent-child join.
- `E_CALC`, `E_LIMIT`, `E_SORT`, `E_AGGREGATE`, `E_JOIN`: normal enumerable operators above or around Parquet nodes.

For every supported parent-child join, also check that projected scan fields map the join keys correctly. In particular, for child tables the join key `__polypheny_parent_row_id` should map to physical column index `1`, not to `0`.

## Simple Scans And Projections

### Q01. Root table scan with limit

Expected plan: one `P_SCAN` for `pon__orders`, wrapped by `PE_CALC`; limit remains above the scan as `E_LIMIT` unless a future rule pushes it.

Check: baseline root-table planning and limit placement.

```sql
SELECT *
FROM pon__orders
LIMIT 10;
```

### Q02. Root table projection

Expected plan: `P_SCAN` should read only projected order columns if projection pushdown applies.

Check: simple projection pushdown on a root table.

```sql
SELECT order_id, status, total_price
FROM pon__orders
LIMIT 10;
```

### Q03. Customer projection

Expected plan: `P_SCAN` for `pon__customers`, with only selected customer fields.

Check: projection over another root table and text/timestamp columns.

```sql
SELECT customer_id, name, country, signup_date
FROM pon__customers
LIMIT 10;
```

### Q04. Product projection

Expected plan: `P_SCAN` for `pon__products`, with only selected product fields.

Check: projection over numeric and text product columns.

```sql
SELECT product_id, name, category, price, stock
FROM pon__products
LIMIT 10;
```

### Q05. Repeated child projection

Expected plan: `P_SCAN` for `pon__orders__items`, with projected child fields only.

Check: projection on a nested repeated child table.

```sql
SELECT order_item_id, product_id, quantity, price
FROM pon__orders__items
LIMIT 10;
```

### Q06. Repeated child structural columns

Expected plan: `P_SCAN` for `pon__orders__items`, keeping the generated row id, parent row id, and ordinal.

Check: scan of generated structural columns used by nested tables.

```sql
SELECT __polypheny_row_id, __polypheny_parent_row_id, __polypheny_elem_ordinal
FROM pon__orders__items
LIMIT 10;
```

### Q07. Nested grandchild projection

Expected plan: `P_SCAN` for `pon__orders__items__discounts`, with projected discount fields.

Check: projection on a second-level nested repeated child table.

```sql
SELECT code, amount
FROM pon__orders__items__discounts
LIMIT 10;
```

### Q08. Direct nested shipping-address projection

Expected plan: `P_SCAN` for `pon__orders__shipping_address`, with projected address fields.

Check: projection on a direct nested child that is not the `items` repeated branch.

```sql
SELECT city, street, zip
FROM pon__orders__shipping_address
LIMIT 10;
```

## Filters Without Joins

### Q09. Root numeric filter

Expected plan: `P_SCAN` with a translatable filter if filter pushdown applies; otherwise `E_CALC` or filter above `PE_CALC`.

Check: root-table numeric predicate.

```sql
SELECT order_id, total_price
FROM pon__orders
WHERE total_price >= 0
LIMIT 10;
```

### Q10. Root nullable text filter

Expected plan: `P_SCAN` plus a status filter when supported.

Check: nullable text predicate on root table.

```sql
SELECT order_id, status
FROM pon__orders
WHERE status IS NOT NULL
LIMIT 10;
```

### Q11. Root timestamp filter

Expected plan: `P_SCAN` plus a timestamp/nullability filter when supported.

Check: timestamp column handling.

```sql
SELECT order_id, order_date
FROM pon__orders
WHERE order_date IS NOT NULL
LIMIT 10;
```

### Q12. Customer text filter

Expected plan: `P_SCAN` for `pon__customers`; text predicate may or may not be pushed.

Check: text filter planning on a root table.

```sql
SELECT customer_id, name, country
FROM pon__customers
WHERE country IS NOT NULL
LIMIT 10;
```

### Q13. Product numeric filter

Expected plan: `P_SCAN` for `pon__products` with a numeric filter when supported.

Check: product scan with numeric predicate and projection.

```sql
SELECT product_id, name, price, stock
FROM pon__products
WHERE price >= 0 AND stock >= 0
LIMIT 10;
```

### Q14. Repeated child numeric filter

Expected plan: `P_SCAN` for `pon__orders__items`, with `quantity` and `price` filters when supported.

Check: filter pushdown on nested repeated table.

```sql
SELECT order_item_id, product_id, quantity, price
FROM pon__orders__items
WHERE quantity > 0 AND price >= 0
LIMIT 10;
```

### Q15. Repeated child ordinal filter

Expected plan: `P_SCAN` for `pon__orders__items`, possibly with a filter on generated ordinal.

Check: generated ordinal field in a filter.

```sql
SELECT __polypheny_parent_row_id, __polypheny_elem_ordinal, product_id
FROM pon__orders__items
WHERE __polypheny_elem_ordinal = 0
LIMIT 10;
```

### Q16. Nested grandchild filter

Expected plan: `P_SCAN` for `pon__orders__items__discounts`, with `amount` filter when supported.

Check: filter on second-level nested repeated table.

```sql
SELECT code, amount
FROM pon__orders__items__discounts
WHERE amount > 0
LIMIT 10;
```

### Q17. Direct nested shipping-address filter

Expected plan: `P_SCAN` for `pon__orders__shipping_address`; filter may remain as `E_CALC` if text predicates are not pushed.

Check: filter on direct nested child branch different from `items`.

```sql
SELECT city, street, zip
FROM pon__orders__shipping_address
WHERE city IS NOT NULL AND zip IS NOT NULL
LIMIT 10;
```

### Q18. Non-pushable expression filter

Expected plan: `P_SCAN` for `pon__products`, with an enumerable calc/filter for the expression if it cannot be pushed.

Check: fallback around a scan when the predicate contains an expression.

```sql
SELECT product_id, name, category
FROM pon__products
WHERE LOWER(category) IS NOT NULL
LIMIT 10;
```

## Supported Parent-Child Joins

### Q19. Orders to items, inner join

Expected plan: `P_JOIN` with `pon__orders` as parent and `pon__orders__items` as child.

Check: basic root parent to repeated child join.

```sql
SELECT o.order_id, i.order_item_id, i.product_id, i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q20. Items to orders, inner join with parent on right

Expected plan: `P_JOIN`; direction detection should identify that the right input is the parent.

Check: supported join with swapped input order.

```sql
SELECT i.order_item_id, i.product_id, o.order_id, o.status
FROM pon__orders__items i
JOIN pon__orders o
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q21. Orders to shipping address

Expected plan: `P_JOIN` with `pon__orders` as parent and `pon__orders__shipping_address` as child.

Check: direct nested child branch that is not `items`.

```sql
SELECT o.order_id, s.city, s.street, s.zip
FROM pon__orders o
JOIN pon__orders__shipping_address s
    ON s.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q22. Shipping address to orders, parent on right

Expected plan: `P_JOIN`; direction detection should identify that the right input is the parent.

Check: swapped direction for a direct nested child branch.

```sql
SELECT s.city, s.zip, o.order_id
FROM pon__orders__shipping_address s
JOIN pon__orders o
    ON s.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q23. Items to discounts, inner join

Expected plan: `P_JOIN` with `pon__orders__items` as parent and `pon__orders__items__discounts` as child.

Check: nested parent to second-level repeated child.

```sql
SELECT i.order_item_id, i.product_id, d.code, d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 10;
```

### Q24. Discounts to items, parent on right

Expected plan: `P_JOIN`; direction detection should identify that the right input is the nested parent.

Check: swapped direction for nested parent-child join.

```sql
SELECT d.code, d.amount, i.order_item_id, i.product_id
FROM pon__orders__items__discounts d
JOIN pon__orders__items i
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 10;
```

### Q25. Reversed equality operands

Expected plan: `P_JOIN`; the equality is written parent key first.

Check: join-key extraction independent of operand order.

```sql
SELECT i.order_item_id, i.quantity, d.code, d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON i.__polypheny_row_id = d.__polypheny_parent_row_id
LIMIT 10;
```

### Q26. Orders to items, left join

Expected plan: `P_JOIN` with join type `LEFT`.

Check: parent-preserving outer join.

```sql
SELECT o.order_id, i.order_item_id, i.product_id
FROM pon__orders o
LEFT JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q27. Items to orders, right join

Expected plan: `P_JOIN` with join type `RIGHT`.

Check: same parent-preserving semantics as Q26 with swapped input order.

```sql
SELECT i.order_item_id, i.product_id, o.order_id
FROM pon__orders__items i
RIGHT JOIN pon__orders o
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q28. Items to discounts, full join

Expected plan: `P_JOIN` with join type `FULL`.

Check: full outer structural join on nested parent and grandchild.

```sql
SELECT i.order_item_id, i.quantity, d.code, d.amount
FROM pon__orders__items i
FULL JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 10;
```

### Q29. Projected full join with filter and limit

Expected plan: `P_JOIN` under `E_LIMIT` and final projection/calc. Both scans should carry the projected fields needed for selected columns, filter columns, and join keys.

Check: the child scan projection bug case; child join key should map to physical column `1`.

```sql
SELECT i.order_item_id, i.quantity, i.price
FROM pon__orders__items i
FULL JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
WHERE i.quantity = 3
LIMIT 10;
```

### Q30. Parent-side filter above structural join

Expected plan: `P_JOIN`; parent filter should be pushed into the scan or join when possible.

Check: filter placement for parent-side predicate.

```sql
SELECT o.order_id, o.total_price, i.product_id, i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
WHERE o.total_price >= 0
LIMIT 10;
```

### Q31. Child-side filter above structural join

Expected plan: `P_JOIN`; child filter should be applied on the child branch or in the joined-row filter.

Check: filter placement for child-side predicate.

```sql
SELECT o.order_id, i.product_id, i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
WHERE i.quantity > 1
LIMIT 10;
```

### Q32. Filters on both sides of structural join

Expected plan: `P_JOIN`; filters may split between parent and child branches.

Check: filter splitting and field-index shifting for joined rows.

```sql
SELECT i.order_item_id, i.quantity, d.code, d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
WHERE i.quantity > 1 AND d.amount > 0
LIMIT 10;
```

### Q33. Aggregate above structural join

Expected plan: `E_AGGREGATE` above `P_JOIN`.

Check: join can still be adapter-level when the final result is an aggregate.

```sql
SELECT count(*)
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
WHERE i.quantity > 0;
```

### Q34. Group by above structural join

Expected plan: `E_AGGREGATE` above `P_JOIN`.

Check: grouped aggregate after adapter-level join.

```sql
SELECT i.product_id, count(*) AS discount_rows
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
GROUP BY i.product_id
LIMIT 10;
```

### Q35. Sort above structural join

Expected plan: `E_SORT` or `E_LIMIT` above `P_JOIN`.

Check: sorting after adapter-level join and projection.

```sql
SELECT o.order_id, i.product_id, i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
ORDER BY i.quantity DESC
LIMIT 10;
```

## Derived Tables And Calc Permutations

### Q36. Projected parent and child derived tables

Expected plan: ideally `P_JOIN`; derived-table projections should be consumed into `P_SCAN` fields.

Check: projection-only `Calc` or `Project` wrappers on both join inputs.

```sql
SELECT o.order_id, i.product_id, i.quantity
FROM (
    SELECT __polypheny_row_id, order_id
    FROM pon__orders
) o
JOIN (
    SELECT __polypheny_parent_row_id, product_id, quantity
    FROM pon__orders__items
) i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q37. Projected nested parent and grandchild derived tables

Expected plan: `P_JOIN`; child `__polypheny_parent_row_id` must map through the projection to physical column `1`.

Check: projection-only calc on the child side of an `items` to `discounts` join.

```sql
SELECT i.quantity, i.price, d.code, d.amount
FROM (
    SELECT __polypheny_row_id, quantity, price
    FROM pon__orders__items
) i
JOIN (
    SELECT __polypheny_parent_row_id, code, amount
    FROM pon__orders__items__discounts
) d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 10;
```

### Q38. Filtered derived table on parent branch

Expected plan: `P_JOIN` if the filter can be attached before join recognition; otherwise fallback indicates a rule gap.

Check: filter/calc wrapper on parent input.

```sql
SELECT o.order_id, i.product_id, i.quantity
FROM (
    SELECT __polypheny_row_id, order_id
    FROM pon__orders
    WHERE total_price >= 0
) o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q39. Filtered derived table on child branch

Expected plan: `P_JOIN` if the child filter and projection can be attached to the scan before join recognition.

Check: child-side `EnumerableCalc` projection/filter wrapper.

```sql
SELECT o.order_id, i.product_id, i.quantity
FROM pon__orders o
JOIN (
    SELECT __polypheny_parent_row_id, product_id, quantity
    FROM pon__orders__items
    WHERE quantity > 1
) i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q40. Final projection hides all child join keys

Expected plan: `P_JOIN`; scan projections still need join keys internally even though final output hides them.

Check: join-key preservation through projection trimming.

```sql
SELECT i.product_id, d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
WHERE d.amount > 0
LIMIT 10;
```

### Q41. Limit inside join input

Expected plan: this is a semantic edge case. If the input limit stays below the join, the adapter join should not silently ignore it.

Check: rule ordering around `E_LIMIT` below a structural join input.

```sql
SELECT i.order_item_id, d.code, d.amount
FROM (
    SELECT __polypheny_row_id, order_item_id
    FROM pon__orders__items
    LIMIT 100
) i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 10;
```

## Multi-Join And Mixed Join Shapes

### Q42. Orders to items to discounts chain

Expected plan: direct structural joins are individually supported. Depending on rule composition, expect either nested `P_JOIN` use or one `P_JOIN` plus a normal join.

Check: whether adapter-level joins compose across multiple levels.

```sql
SELECT o.order_id, i.product_id, d.code, d.amount
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 10;
```

### Q43. Structural join plus product lookup

Expected plan: `orders` to `items` may become `P_JOIN`; `items` to `products` should be a normal join because it uses user columns.

Check: mixed adapter-level and fallback join planning.

```sql
SELECT o.order_id, i.quantity, p.name, p.category
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
JOIN pon__products p
    ON p.product_id = i.product_id
LIMIT 10;
```

### Q44. Customer to orders to items

Expected plan: `customers` to `orders` is a normal user-column join; `orders` to `items` is structurally supported if the planner can isolate it.

Check: structural join recognition inside a larger query with a root-root user join.

```sql
SELECT c.customer_id, c.country, o.order_id, i.product_id
FROM pon__customers c
JOIN pon__orders o
    ON o.customer_id = c.customer_id
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q45. Orders with two direct children

Expected plan: each root-to-child join is structurally valid, but composition may fall back after one `P_JOIN`.

Check: sibling child branches under the same parent.

```sql
SELECT o.order_id, i.product_id, s.city, s.zip
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
JOIN pon__orders__shipping_address s
    ON s.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

## Unsupported Or Fallback Plans

### Q46. Customers to orders by user key

Expected plan: no `P_JOIN`; use normal `E_JOIN` or equivalent.

Check: root-root user-column join must not be mistaken for structural join.

```sql
SELECT c.customer_id, c.name, o.order_id, o.status
FROM pon__customers c
JOIN pon__orders o
    ON o.customer_id = c.customer_id
LIMIT 10;
```

### Q47. Items to products by product id

Expected plan: no `P_JOIN`; use normal join.

Check: nested table joined to root lookup table by user column.

```sql
SELECT i.order_item_id, i.quantity, p.name, p.price
FROM pon__orders__items i
JOIN pon__products p
    ON p.product_id = i.product_id
LIMIT 10;
```

### Q48. Orders directly to discounts

Expected plan: no single `P_JOIN`; discounts are not a direct child of orders.

Check: ancestor-to-grandchild direct join is rejected.

```sql
SELECT o.order_id, d.code, d.amount
FROM pon__orders o
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q49. Sibling child join

Expected plan: no `P_JOIN`; items and shipping address are siblings, not parent and child.

Check: same-parent sibling relationship is rejected.

```sql
SELECT i.product_id, s.city, s.zip
FROM pon__orders__items i
JOIN pon__orders__shipping_address s
    ON s.__polypheny_parent_row_id = i.__polypheny_parent_row_id
LIMIT 10;
```

### Q50. Same-table self join

Expected plan: no `P_JOIN`; use normal self-join.

Check: self-join on generated parent row id is not a structural parent-child join.

```sql
SELECT i1.product_id AS product_1, i2.product_id AS product_2
FROM pon__orders__items i1
JOIN pon__orders__items i2
    ON i1.__polypheny_parent_row_id = i2.__polypheny_parent_row_id
LIMIT 10;
```

### Q51. Parent-child tables with wrong generated keys

Expected plan: no `P_JOIN`; child primary key is not the parent reference key.

Check: key-role validation in `supportedDirection()`.

```sql
SELECT o.order_id, i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_row_id = o.__polypheny_row_id
LIMIT 10;
```

### Q52. Parent-child tables joined by user columns

Expected plan: no `P_JOIN`; structural keys are not used.

Check: user-column join between parent and child falls back.

```sql
SELECT o.order_id, i.order_item_id
FROM pon__orders o
JOIN pon__orders__items i
    ON o.order_id = i.order_item_id
LIMIT 10;
```

### Q53. Multi-key join condition

Expected plan: no `P_JOIN` if the extra equality remains part of the join condition.

Check: `supportedDirection()` requires exactly one equi-key on each side.

```sql
SELECT o.order_id, i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
   AND i.__polypheny_elem_ordinal = o.order_id
LIMIT 10;
```

### Q54. Non-equi join condition

Expected plan: no `P_JOIN`.

Check: non-equi structural condition is rejected.

```sql
SELECT o.order_id, i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id <> o.__polypheny_row_id
LIMIT 10;
```

### Q55. OR join condition

Expected plan: no `P_JOIN`.

Check: disjunctive join condition is rejected.

```sql
SELECT o.order_id, i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
    OR i.__polypheny_elem_ordinal = 0
LIMIT 10;
```

### Q56. EXISTS over structural relationship

Expected plan: no `P_JOIN` unless a future semi-join rule explicitly supports this shape.

Check: correlated/semi-join planning around nested tables.

```sql
SELECT o.order_id
FROM pon__orders o
WHERE EXISTS (
    SELECT 1
    FROM pon__orders__items i
    WHERE i.__polypheny_parent_row_id = o.__polypheny_row_id
)
LIMIT 10;
```

### Q57. NOT EXISTS over structural relationship

Expected plan: no `P_JOIN` unless a future anti-join rule explicitly supports this shape.

Check: anti-join or correlated fallback planning.

```sql
SELECT o.order_id
FROM pon__orders o
WHERE NOT EXISTS (
    SELECT 1
    FROM pon__orders__items i
    WHERE i.__polypheny_parent_row_id = o.__polypheny_row_id
)
LIMIT 10;
```

