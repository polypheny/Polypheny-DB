# Polypheny REST API

### Available requests

#### GET

Allowed URL parts: Projection, Grouping, Sort, Filter, Limit, Offset

#### POST

Allowed URL parts: None

#### PATCH

Allowed URL parts: Filter

#### DELETE

Allowed URL parts: Filter

### URL

#### Some general definitions
```
qualified_table := <schema> "." <table>
qualified_column := <schema> "." <table> "." <column>
alias_or_column := <qualified_column> | <alias>
```

#### Overall URL format
```
URL := /restapi/v1/res/<resources>
resources := <qualified_table> [ "," <qualified_table> ...]
```

#### Projections and aggregate functions
```
"_project=" <projection> [ "," <projection> ...]
projection := <qualified_column> [ "@" <alias> [ "(" <function> ")" ] ]
function := "COUNT" | "MAX" | "MIN" | "AVG" | "SUM"
```

#### Group by
```
"_groupby=" <alias_or_column> [ "," <alias_or_column> ...]
```
Important: `_groupby` must be used together with `_project`. It must include all columns that do not have an aggregate function applied.

#### Sorting
```
"_sort=" <sort_entry> [ "," <sort_entry> ...]
sort_entry := <alias_or_column> [ "@" ( "ASC" | "DESC" ) ]
```
Not specifying a sort direction means ASC.

#### Filters
```
<alias_or_column> = <operation> <literal_value>
<operation> := "=" | "<=" | ">=" | "!=" | "%" | "!%"
```

#### Limit and Offset
```
"_limit=" <limit>
"_offset=" <offset>
```

### Body

#### Content Type
The content type for POST/PATCH requests must be `application/json`.

#### Formatting

```json
{
  "data": [
    { <qualified_column>: <value>, ...},
    { ... }
  ]
}
```
