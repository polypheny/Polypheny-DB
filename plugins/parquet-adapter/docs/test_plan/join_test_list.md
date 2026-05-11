# Join Test List

This document lists SQL queries that should be used to test adapter-level Parquet joins and fallback join behavior.

The examples assume a normalized Parquet adapter with these generated tables:

- `pon__orders` - root table
- `pon__orders__items` - direct child table of `pon__orders`
- `pon__orders__items__discounts` - direct child table of `pon__orders__items`

Expected adapter-level support means the physical plan should contain `ParquetRelJoin`.
Unsupported cases should still execute through the normal Polypheny join path and should not produce `ParquetRelJoin`.

## Supported Adapter-Level Joins

### 1. Root parent - direct child, parent on left, inner join

Expected: supported. The plan should contain `ParquetRelJoin`.

```sql
SELECT
    o.__polypheny_row_id AS order_row_id,
    o.order_id,
    i.__polypheny_row_id AS item_row_id,
    i.__polypheny_parent_row_id AS item_parent_row_id,
    i.product_id,
    i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 2. Root parent - direct child, parent on right, inner join

Expected: supported. This validates that the rule detects parent-child direction when the child table is the left input.

```sql
SELECT
    i.__polypheny_row_id AS item_row_id,
    i.product_id,
    o.__polypheny_row_id AS order_row_id,
    o.order_id
FROM pon__orders__items i
JOIN pon__orders o
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 3. Root parent - direct child, equality written in reversed order

Expected: supported. This validates that the rule accepts the same equi-join when the operands are reversed.

```sql
SELECT
    o.order_id,
    i.product_id,
    i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON o.__polypheny_row_id = i.__polypheny_parent_row_id
LIMIT 20;
```

### 4. Nested parent - direct nested child, parent on left, inner join

Expected: supported. This is the main nested-parent case: `items` is parent, `discounts` is direct child.

```sql
SELECT
    i.__polypheny_row_id AS item_row_id,
    d.__polypheny_row_id AS discount_row_id,
    d.__polypheny_parent_row_id AS discount_parent_row_id,
    i.product_id,
    d.code,
    d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

### 5. Nested parent - direct nested child, parent on right, inner join

Expected: supported. This validates nested parent-child direction when the child table is the left input.

```sql
SELECT
    d.__polypheny_row_id AS discount_row_id,
    d.code,
    d.amount,
    i.__polypheny_row_id AS item_row_id,
    i.product_id
FROM pon__orders__items__discounts d
JOIN pon__orders__items i
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

### 6. Root parent - direct child, left join preserves parent rows
PROBLEM

Expected: supported. Parent is on the left, so unmatched parent rows can be emitted by adapter runtime.

```sql
SELECT
    o.order_id,
    i.product_id,
    i.quantity
FROM pon__orders o
LEFT JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 7. Root parent - direct child, right join preserves parent rows

Expected: supported. Parent is on the right, so this is the same preservation direction as the previous test but with swapped input order.

```sql
SELECT
    i.product_id,
    i.quantity,
    o.order_id
FROM pon__orders__items i
RIGHT JOIN pon__orders o
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 8. Nested parent - direct nested child, left join preserves nested parent rows

Expected: supported. This checks unmatched `items` rows when an item has no discount.

```sql
SELECT
    i.product_id,
    d.code,
    d.amount
FROM pon__orders__items i
LEFT JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

### 9. Nested parent - direct nested child, right join preserves nested parent rows

Expected: supported. Parent is on the right, so the runtime should still preserve unmatched parent rows.

```sql
SELECT
    d.code,
    d.amount,
    i.product_id
FROM pon__orders__items__discounts d
RIGHT JOIN pon__orders__items i
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

### 10. Root parent - direct child, full join

Expected: supported by the rule. Runtime preserves unmatched parent rows. It does not do an independent child-side outer scan outside the parent path.

```sql
SELECT
    o.order_id,
    i.product_id,
    i.quantity
FROM pon__orders o
FULL JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 11. Nested parent - direct nested child, full join

Expected: supported by the rule. Runtime preserves unmatched parent rows. It does not do an independent child-side outer scan outside the parent path.

```sql
SELECT
    i.product_id,
    d.code,
    d.amount
FROM pon__orders__items i
FULL JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

### 12. Root parent - direct child with filter above join

Expected: supported. The join should still become `ParquetRelJoin`; the filter may be pushed into the adapter join if translatable.

```sql
SELECT
    o.order_id,
    i.product_id,
    i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
WHERE i.quantity > 1
LIMIT 20;
```

### 13. Nested parent - direct nested child with child-side filter

Expected: supported. The join should still become `ParquetRelJoin`; the discount filter should be applied on the child side or as an adapter-level joined-row filter.

```sql
SELECT
    i.product_id,
    d.code,
    d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
WHERE d.amount > 0
LIMIT 20;
```

### 14. Nested parent - direct nested child with parent-side filter

Expected: supported. The join should still become `ParquetRelJoin`; the item filter should be applied before or during child expansion when possible.

```sql
SELECT
    i.product_id,
    i.quantity,
    d.code,
    d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
WHERE i.quantity > 1
LIMIT 20;
```

### 15. Nested parent - direct nested child with projected columns

Expected: supported. This validates plans where `Calc` or `Project` nodes appear between the join and the Parquet scans.

```sql
SELECT
    i.__polypheny_row_id AS item_id,
    d.__polypheny_row_id AS discount_id,
    d.__polypheny_parent_row_id AS discount_parent_id,
    d.__polypheny_elem_ordinal AS discount_ordinal,
    i.product_id,
    d.code,
    d.amount
FROM pon__orders__items i
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

### 16. Two adapter-level joins in one query, root to child and child to grandchild

Expected: both direct joins are individually supported. The ideal plan has `ParquetRelJoin` for both direct parent-child joins, but this should be verified because rule ordering can affect the final shape.

```sql
SELECT
    o.order_id,
    i.product_id,
    d.code,
    d.amount
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = i.__polypheny_row_id
LIMIT 20;
```

## Unsupported Or Fallback Join Shapes

### 17. Root parent - grandchild directly

Expected: not supported as one adapter-level join. `discounts` points to `items`, not directly to `orders`.

```sql
SELECT
    o.order_id,
    d.code,
    d.amount
FROM pon__orders o
JOIN pon__orders__items__discounts d
    ON d.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 18. Ancestor-descendant join beyond direct parent-child

Expected: not supported as one adapter-level join. This tries to connect root rows directly to descendant discount rows by structural row-id prefix, not by direct `PRIMARY_KEY` to `PARENT_KEY`.

```sql
SELECT
    o.order_id,
    d.code,
    d.amount
FROM pon__orders o
JOIN pon__orders__items__discounts d
    ON d.__polypheny_row_id LIKE o.__polypheny_row_id || '/%'
LIMIT 20;
```

### 19. Child-child join inside the same parent

Expected: not supported at adapter level today. This requires a schema with another repeated child table such as `pon__orders__payments`.

```sql
SELECT
    i.product_id,
    p.payment_id,
    p.amount
FROM pon__orders__items i
JOIN pon__orders__payments p
    ON p.__polypheny_parent_row_id = i.__polypheny_parent_row_id
LIMIT 20;
```

### 20. Same table self-join

Expected: not supported at adapter level. This is a normal relational self-join.

```sql
SELECT
    i1.product_id AS product_1,
    i2.product_id AS product_2
FROM pon__orders__items i1
JOIN pon__orders__items i2
    ON i1.__polypheny_parent_row_id = i2.__polypheny_parent_row_id
LIMIT 20;
```

### 21. Parent-child tables but wrong generated keys

Expected: not supported at adapter level. The child must join from `PARENT_KEY` to parent `PRIMARY_KEY`.

```sql
SELECT
    o.order_id,
    i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_row_id = o.__polypheny_row_id
LIMIT 20;
```

### 22. Parent-child tables joined on user columns

Expected: not supported at adapter level. Adapter-level nested joins only support generated structural keys.

```sql
SELECT
    o.order_id,
    i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON o.order_id = i.order_item_id
LIMIT 20;
```

If `order_item_id` is not present in the dataset, replace it with another compatible user column. The important part is that the join does not use `__polypheny_parent_row_id = __polypheny_row_id`.

### 23. Multi-key join condition

Expected: not supported as adapter-level join when the extra condition is part of the join condition. Put additional filters in `WHERE` if the structural join itself should still be recognized.

```sql
SELECT
    o.order_id,
    i.product_id,
    i.quantity
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
   AND i.__polypheny_elem_ordinal = 0
LIMIT 20;
```

### 24. Non-equi join

Expected: not supported at adapter level. Only a single equality between parent `PRIMARY_KEY` and child `PARENT_KEY` is supported.

```sql
SELECT
    o.order_id,
    i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id <> o.__polypheny_row_id
LIMIT 20;
```

### 25. OR join condition

Expected: not supported at adapter level. The join condition is not a single structural equi-join. (contains OR)

```sql
SELECT
    o.order_id,
    i.product_id
FROM pon__orders o
JOIN pon__orders__items i
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
    OR i.__polypheny_elem_ordinal = 0
LIMIT 20;
```

### 26. Cross join

Expected: not supported at adapter level. There is no structural parent-child equality condition.

```sql
SELECT
    o.order_id,
    i.product_id
FROM pon__orders o
CROSS JOIN pon__orders__items i
LIMIT 20;
```

### 27. Semi join through EXISTS

Expected: not supported as `ParquetRelJoin`. This may become a semi-join or correlated plan in Polypheny.

```sql
SELECT
    o.order_id
FROM pon__orders o
WHERE EXISTS (
    SELECT 1
    FROM pon__orders__items i
    WHERE i.__polypheny_parent_row_id = o.__polypheny_row_id
)
LIMIT 20;
```

### 28. Anti join through NOT EXISTS

Expected: not supported as `ParquetRelJoin`. This may become an anti-join or correlated plan in Polypheny.

```sql
SELECT
    o.order_id
FROM pon__orders o
WHERE NOT EXISTS (
    SELECT 1
    FROM pon__orders__items i
    WHERE i.__polypheny_parent_row_id = o.__polypheny_row_id
)
LIMIT 20;
```

### 29. Join between different Parquet sources

Expected: not supported at adapter level. This requires another adapter/table prefix such as `pon2__orders`.

```sql
SELECT
    o.order_id,
    o2.order_id AS other_order_id
FROM pon__orders o
JOIN pon2__orders o2
    ON o.order_id = o2.order_id
LIMIT 20;
```

### 30. Join between normalized Parquet table and non-Parquet table

Expected: not supported at adapter level. This is a normal Polypheny join.

```sql
SELECT
    o.order_id,
    c.customer_id
FROM pon__orders o
JOIN customers c
    ON o.customer_id = c.customer_id
LIMIT 20;
```

### 31. Child-side preserving outer join

Expected: accepted by the rule for the structural join, but child rows normally cannot exist without a parent in normalized data. This should be tested mainly to verify runtime semantics and null handling.

```sql
SELECT
    i.product_id,
    o.order_id
FROM pon__orders__items i
LEFT JOIN pon__orders o
    ON i.__polypheny_parent_row_id = o.__polypheny_row_id
LIMIT 20;
```


Failed query numbers:

12, 13, 14, 19, 23, 29, 30

Failure reasons:

12, 13, 14, 23: row-type mismatch errors.
19, 29, 30: Entity not found errors.
