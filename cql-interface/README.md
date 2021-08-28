# Contextual Query Language

This implementation of CQL for Polypheny was part of [Vishal Dalwadi's](https://github.com/VishalDalwadi)
Google Summer of Code 2021 project **Support for Contextual Query Language**. The features implemented are
explained below.

The initial design for CQL implementation was to allow users to create JARs that contain the context set
information. These JARs could then be found by a plugin manager and the CQL execution engine could use the
context sets to convert the query into relation algebra.

However, after careful consideration, we came upon the current design which is much simpler (both for
understanding and implementation). In the current design, the schema takes on the role of Context Set.
The schema has tables which in-turn have columns. These two take on the role of index (as specified in the
CQL standard spec). `Filters` are operations taking place on columns and `Relations` are the output of combine
operation ( Joins or Set Operations ) on tables.

### Table of Contents

* **[Basics](#basics)**
* **[Syntax](#syntax)**
* **[Future Plans and Nice-to-have features](#future-plans-and-nice-to-have-features)**

### Basics

Polypheny, currently, executes CQL query on its [REST interface](https://github.com/polypheny/Polypheny-DB/tree/master/rest-interface)
at route `GET /restapi/v1/cql`. The handler assumes the entire body to be the CQL query.

Polypheny's CQL implementation makes some changes and adds many extensions to the [CQL Specification](https://www.loc.gov/standards/sru/cql/spec.html).
It does not have prefix assignments or search-term-only filters. It also introduces some more keywords to add
more details in the query.

Polypheny's CQL implementation uses fully qualified names instead of indices in CQL standard. For example,
column names like `public.emps.emp` and table names like `public.emps`.

### Syntax

Polypheny's CQL Implementation consists of four major parts: Filters, Relations, Sort Specifications and Projections.
The format of each of these is discussed later. The basic syntax of a query consisting of these four parts is shown
below. The parser is case-insensitive when it comes to keywords (such as modifiers, boolean operators or comparison
operators) but is case-sensitive when it comes to names and literals (such column names, table names or literal 
values in filters). 

```
CQL Query:
    (
        Filters
    |
        relation Relation
    |
        Filters relation Relation
    )
    [ sortby SortSpecification ]
    [ project Projection ]

Filters:
    ( Filters ) | Filter BooleanOp Filter | Filter

Relation:
    Table Combiner Relation | Table

SortSpecification:
    Column Modifiers

Projection:
    Column Modifiers

Filter:
    Column ComparisonOp ( Column | Literal value with or without double quotes )

BooleanOp:
    (AND | OR | NOT | PROX)
    Modifiers

Combiner:
    (AND | OR)
    Modifiers

Column:
    SchemaName.TableName.ColumnName

Modifiers:
    ( Modifier )*

Modifier:
    / ModifierName
    [ ComparisonOp ModifierValue ]

ComparisonOp:
    (= | == | <> | < | > | <= | >= | NamedComparator)
    Modifiers

NamedComparator:
    String containing only alphabets.

Table:
    SchemaName.TableName

SchemaName, TableName, ColumnName:
    String containing only alphabets or underscores (_).

ModifierName:
    String without double quotes, white spaces, escaped double quote, parenthesis, =, <, > or /.

ModifierValue:
    String value with or without double quotes.
```

- `Filters`: Filters are used to do comparisons on a column in the `Relation`. They are similar to SQL's WHERE clause.
If `Relation` is specified, the column must be from the relation specified. Comparisons can be between literal
values or columns; however, support for comparisons with columns is still underway. Comparisons like equals,
not equal, less than, greater than, less than or equals and greater than or equals can be done with support for
more (between, any, all, etc.) to be added later. Multiple filters can be separated by AND, OR, NOT or PROX boolean
operators; however, support for PROX is still underway. Boolean operators' precedence depend on their position; i.e.
First occurring boolean operator has higher precedence than those occurring later. However, this can be change by
using parenthesis.

- `Relation`: Relation is the final table that the query would be executed on. So the relation is a combination of
multiple tables. The combination operation can be a join, union, intersection, set-difference, etc; however, 
implementation of set operations as combiners is still underway. The actual combiner keyword used is AND or OR.
For joins, AND means a INNER join, whereas OR means a FULL, LEFT or RIGHT join. The combiner also takes modifiers
used to modify its execution. The two modifiers currently supported are: `null` and `on`.

  The `null` modifier is to be used with OR combiner to specify which of the rows can be null. Possible values of
  `null` modifier are: `both` (FULL join), `right` (LEFT join) or `left` (RIGHT join).

  The `on` modifier is used to specify the columns to join on. It only works if the column(s) belong to both the
tables. Possible values of `on` modifier are: `all` (finds the common columns between two tables; Default for AND),
`none` (Default for OR), comma-separated list of column names (for example, 'name,id').
  
- `Sort Specification`: Sort specification is used to specify a space separate column list on which to sort the query
output.

- `Projection`: Projection is used to specify the columns for the result. It is also used for aggregations and grouping.

Example queries:

Consider a schema "public" with tables "employee" and "dept" defined as follows:

```
CREATE TABLE public.dept(
deptno TINYINT NOT NULL,
deptname VARCHAR(30) NOT NULL,
PRIMARY KEY (deptno) );

CREATE TABLE public.employee(
empno INTEGER NOT NULL,
empname VARCHAR(20) NOT NULL,
salary REAL NOT NULL,
deptno TINYINT NOT NULL,
married BOOLEAN NOT NULL,
dob DATE NOT NULL,
joining_date DATE NOT NULL,
PRIMARY KEY (empno) );
```

Then the following CQL queries can be executed on the schema.

- Find employee named "Loki":
`public.employee.empname == "Loki"`

- Find all employees in the HR department that are married:
`public.dept.deptname == "HR" and public.employee.married == TRUE`

- Find all employees from the HR or IT department:
`public.dept.deptname == "HR" or public.dept.deptname == "IT" relation public.employee and public.dept`

- Find all employees from all departments except HR:
`public.employee.empno >= 1 NOT public.dept.deptname == "HR"`

- Count the number of employees:
`relation public.employee project public.employee.empno/count`

- Get all the employee names sorted by date of birth:
`relation public.employee sortby public.employee.dob project public.employee.empname`

- Count the number of employees in each department:
`relation public.employee project public.employee.empno/count public.employee.deptno`

### Future Plans and Nice-to-have features

- Optimize Combiner's `getCommonColumns` by creating a cache.
- Support for Column Filters.
- Support for Set Operations.
- Support for proximity boolean operator.
- Support for querying the result of a query.
- Modifiers for Sorting, Projection and Filtering.
- Increase test coverage.
