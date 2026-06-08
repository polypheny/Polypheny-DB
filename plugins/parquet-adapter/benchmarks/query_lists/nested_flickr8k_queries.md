# Nested Data Queries

## Q01: Full Flickr8k Row Count

Find how many Flickr8k images are in the dataset.

```sql
SELECT count(*) AS row_count
FROM public.ff__flickr8k;
```

## Q02: Image Id Projection

Find all image identifiers without reading captions or image bytes.

```sql
SELECT id
FROM public.ff__flickr8k;
```

## Q03: Ordered Image Id Sample

Find a deterministic sample of image identifiers without reading nested
captions or image bytes.

```sql
SELECT id
FROM public.ff__flickr8k
ORDER BY id
LIMIT 100;
```

## Q04: Repeated Caption List Projection

Find the repeated caption list for every image without reading image bytes.

```sql
SELECT captions
FROM public.ff__flickr8k;
```

## Q05: Image Ids With Caption Lists

Find image identifiers together with their repeated caption lists, while
excluding image bytes.

```sql
SELECT id, captions
FROM public.ff__flickr8k;
```

## Q06: Ordered Image Ids With Caption Lists Sample

Find a deterministic sample of image identifiers and repeated caption lists,
while excluding image bytes.

```sql
SELECT id, captions
FROM public.ff__flickr8k
ORDER BY id
LIMIT 100;
```

## Q07: Binary Image Projection

Find image identifiers together with the binary JPEG bytes, while excluding
caption lists.

```sql
SELECT id, image
FROM public.ff__flickr8k;
```

## Q08: Binary Image Only Projection

Find only the binary JPEG bytes to measure the cost of reading the large image
column without metadata or captions.

```sql
SELECT id, "image"
FROM public.ff__flickr8k;
```

## Q09: Full Flickr8k Row Projection

Find image identifiers, repeated caption lists, and binary JPEG bytes together.

```sql
SELECT id, captions, "image"
FROM public.ff__flickr8k;
```

## Q10: Filtered Caption Projection Without Image Bytes

Find image identifiers and repeated caption lists for one filename range,
while excluding image bytes.

```sql
SELECT id, captions
FROM public.ff__flickr8k
WHERE id >= '1000000000'
  AND id < '2000000000'
ORDER BY id;
```
