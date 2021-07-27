---
layout: docs
title: SQL language
permalink: /docs/reference.html
---

<style>
.container {
  width: 400px;
  height: 26px;
}
.gray {
  width: 60px;
  height: 26px;
  background: gray;
  float: left;
}
.r15 {
  width: 40px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r12 {
  width: 10px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r13 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r2 {
  width: 2px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 20px;
}
.r24 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 20px;
}
.r35 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 30px;
}
</style>

The page describes the SQL dialect recognized by Polypheny-DB's default SQL parser.

## Grammar

SQL grammar in [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_Form)-like
form.

{% highlight sql %}
statement:
      alterStatement
  |   explain
  |   describe
  |   insert
  |   update
  |   merge
  |   delete
  |   query

alterStatement:
ALTER ( SYSTEM | SESSION ) SET identifier '=' expression | ALTER ( SYSTEM | SESSION ) RESET identifier | ALTER ( SYSTEM | SESSION ) RESET ALL | ALTER SCHEMA [ databaseName . ] schemaName RENAME TO newSchemaName  
| ALTER SCHEMA [ databaseName . ] schemaName OWNER TO userName  
| ALTER TABLE [ databaseName . ] [ schemaName . ] tableName RENAME TO newTableName  
| ALTER TABLE [ databaseName . ] [ schemaName . ] tableName OWNER TO userName | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName RENAME COLUMN columnName TO newColumnName | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName DROP COLUMN columnName | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD COLUMN columnName type [ NULL | NOT NULL ] [DEFAULT defaultValue] [(BEFORE | AFTER) columnName]
| ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD COLUMN columnName physicalName AS name [DEFAULT defaultValue] [(BEFORE | AFTER) columnName]
| ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName SET NOT NULL | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName DROP NOT NULL | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName SET COLLATION collation | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName SET DEFAULT value | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName DROP DEFAULT | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName SET TYPE type | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY COLUMN columnName SET POSITION ( BEFORE | AFTER ) columnName | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD PRIMARY KEY ( columnName | '(' columnName [ , columnName ]* ')' )
| ALTER TABLE [ databaseName . ] [ schemaName . ] tableName DROP PRIMARY KEY | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD CONSTRAINT constraintName UNIQUE ( columnName| '(' columnName [ , columnName ]* ')' )
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName DROP CONSTRAINT constraintName
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD CONSTRAINT foreignKeyName FOREIGN KEY ( columnName | '(' columnName [ , columnName ]* ')' ) REFERENCES [ databaseName . ] [ schemaName . ] tableName '(' columnName [ , columnName ]* ')' [ ON UPDATE ( CASCADE | RESTRICT | SET NULL | SET DEFAULT ) ] [ ON DELETE ( CASCADE | RESTRICT | SET NULL | SET DEFAULT ) ]
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName DROP FOREIGN KEY foreignKeyName
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD [UNIQUE] INDEX indexName ON ( columnName | '(' columnName [ , columnName ]* ')' ) [ USING indexMethod ] [ ON STORE storeName ]
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName DROP INDEX indexName
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName ADD PLACEMENT [( columnName | '(' columnName [ , columnName ]* ')' )] ON STORE storeUniqueName [ WITH PARTITIONS '(' partitionId [ , partitionId ]* ')']
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY PLACEMENT ( ADD | DROP ) COLUMN columnName ON STORE storeUniqueName
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY PLACEMENT '(' columnName [ , columnName ]* ')' ON STORE storeUniqueName 
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName DROP PLACEMENT ON STORE storeUniqueName
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName PARTITION BY ( HASH | RANGE | LIST) '(' columnName ')' [PARTITIONS numPartitions | with (partitionName1, partitionName2 [, partitionNameN]* ) ]
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MERGE PARTITIONS
     | ALTER TABLE [ databaseName . ] [ schemaName . ] tableName MODIFY PARTITIONS '(' partitionId [ , partitionId ]* ')' ON STORE storeName
     | ALTER CONFIG key SET value
     | ALTER ADAPTERS ADD uniqueName USING adapterClass WITH config 
     | ALTER ADAPTERS DROP uniqueName
     | ALTER INTERFACES ADD uniqueName USING clazzName WITH config 
     | ALTER INTERFACES DROP uniqueName

explain:
      EXPLAIN PLAN
      [ WITH TYPE | WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION ]
      [ EXCLUDING ATTRIBUTES | INCLUDING [ ALL ] ATTRIBUTES ]
      [ AS JSON | AS XML ]
      FOR ( query | insert | update | merge | delete )

describe:
      DESCRIBE DATABASE databaseName
   |  DESCRIBE CATALOG [ databaseName . ] catalogName
   |  DESCRIBE SCHEMA [ [ databaseName . ] catalogName ] . schemaName
   |  DESCRIBE [ TABLE ] [ [ [ databaseName . ] catalogName . ] schemaName . ] tableName [ columnName ]
   |  DESCRIBE [ STATEMENT ] ( query | insert | update | merge | delete )

insert:
      ( INSERT | UPSERT ) INTO tablePrimary
      [ '(' column [, column ]* ')' ]
      query

update:
      UPDATE tablePrimary
      SET assign [, assign ]*
      [ WHERE booleanExpression ]

assign:
      identifier '=' expression

merge:
      MERGE INTO tablePrimary [ [ AS ] alias ]
      USING tablePrimary
      ON booleanExpression
      [ WHEN MATCHED THEN UPDATE SET assign [, assign ]* ]
      [ WHEN NOT MATCHED THEN INSERT VALUES '(' value [ , value ]* ')' ]

delete:
      DELETE FROM tablePrimary [ [ AS ] alias ]
      [ WHERE booleanExpression ]

query:
      values
  |   WITH withItem [ , withItem ]* query
  |   {
          select
      |   selectWithoutFrom
      |   query UNION [ ALL | DISTINCT ] query
      |   query EXCEPT [ ALL | DISTINCT ] query
      |   query MINUS [ ALL | DISTINCT ] query
      |   query INTERSECT [ ALL | DISTINCT ] query
      }
      [ ORDER BY orderItem [, orderItem ]* ]
      [ LIMIT [ start, ] { count | ALL } ]
      [ OFFSET start { ROW | ROWS } ]
      [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]

withItem:
      name
      [ '(' column [, column ]* ')' ]
      AS '(' query ')'

orderItem:
      expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]

select:
      SELECT [ STREAM ] [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]
      [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]

selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition:
      ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

columnDecl:
      column type [ NOT NULL ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'
  |   GROUPING SETS '(' groupItem [, groupItem ]* ')'

windowRef:
      windowName
  |   windowSpec

windowSpec:
      [ windowName ]
      '('
      [ ORDER BY orderItem [, orderItem ]* ]
      [ PARTITION BY expression [, expression ]* ]
      [
          RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING }
      |   ROWS numericExpression { PRECEDING | FOLLOWING }
      ]
      ')'
{% endhighlight %}

In *insert*, if the INSERT or UPSERT statement does not specify a
list of target columns, the query must have the same number of
columns as the target table, except in certain
[conformance levels]({{ site.apiRoot }}/org/polypheny/db/sql/validate/SqlConformance.html#isInsertSubsetColumnsAllowed--).

In *merge*, at least one of the WHEN MATCHED and WHEN NOT MATCHED clauses must
be present.

*tablePrimary* may only contain an EXTEND clause in certain
[conformance levels]({{ site.apiRoot }}/org/polypheny/db/sql/validate/SqlConformance.html#allowExtend--);
in those same conformance levels, any *column* in *insert* may be replaced by
*columnDecl*, which has a similar effect to including it in an EXTEND clause.

In *orderItem*, if *expression* is a positive integer *n*, it denotes
the <em>n</em>th item in the SELECT clause.

In *query*, *count* and *start* may each be either an unsigned integer literal
or a dynamic parameter whose value is an integer.

An aggregate query is a query that contains a GROUP BY or a HAVING
clause, or aggregate functions in the SELECT clause. In the SELECT,
HAVING and ORDER BY clauses of an aggregate query, all expressions
must be constant within the current group (that is, grouping constants
as defined by the GROUP BY clause, or constants), or aggregate
functions, or a combination of constants and aggregate
functions. Aggregate and grouping functions may only appear in an
aggregate query, and only in a SELECT, HAVING or ORDER BY clause.

A scalar sub-query is a sub-query used as an expression.
If the sub-query returns no rows, the value is NULL; if it
returns more than one row, it is an error.

IN, EXISTS and scalar sub-queries can occur
in any place where an expression can occur (such as the SELECT clause,
WHERE clause, ON clause of a JOIN, or as an argument to an aggregate
function).

An IN, EXISTS or scalar sub-query may be correlated; that is, it
may refer to tables in the FROM clause of an enclosing query.

*selectWithoutFrom* is equivalent to VALUES,
but is not standard SQL and is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/polypheny/db/sql/validate/SqlConformance.html#isFromRequired--).

MINUS is equivalent to EXCEPT,
but is not standard SQL and is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/polypheny/db/sql/validate/SqlConformance.html#isMinusAllowed--).

CROSS APPLY and OUTER APPLY are only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/polypheny/db/sql/validate/SqlConformance.html#isApplyAllowed--).

"LIMIT start, count" is equivalent to "LIMIT count OFFSET start"
but is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/polypheny/db/sql/validate/SqlConformance.html#isLimitStartCountAllowed--).

## Keywords

The following is a list of SQL keywords. Reserved keywords are **bold**.

{% comment %} start {% endcomment %} A,
**ABS**, ABSENT, ABSOLUTE, ACTION, ADA, ADAPTERS, ADD, ADMIN, AFTER,
**ALL**,
**ALLOCATE**,
**ALLOW**,
**ALTER**, ALWAYS,
**AND**,
**ANY**, APPLY,
**ARCHIVE**,
**ARE**,
**ARRAY**,
**ARRAY_MAX_CARDINALITY**,
**AS**, ASC,
**ASENSITIVE**, ASSERTION, ASSIGNMENT,
**ASYMMETRIC**,
**AT**,
**ATOMIC**, ATTRIBUTE, ATTRIBUTES,
**AUTHORIZATION**,
**AVG**, BEFORE,
**BEGIN**,
**BEGIN_FRAME**,
**BEGIN_PARTITION**, BERNOULLI,
**BETWEEN**,
**BIGINT**,
**BINARY**,
**BIT**,
**BLOB**,
**BOOLEAN**,
**BOTH**, BREADTH,
**BY**, C,
**CALL**,
**CALLED**,
**CARDINALITY**, CASCADE,
**CASCADED**,
**CASE**,
**CAST**, CATALOG, CATALOG_NAME,
**CEIL**,
**CEILING**, CENTURY, CHAIN,
**CHAR**,
**CHARACTER**, CHARACTERISTICS, CHARACTERS,
**CHARACTER_LENGTH**, CHARACTER_SET_CATALOG, CHARACTER_SET_NAME, CHARACTER_SET_SCHEMA,
**CHAR_LENGTH**,
**CHECK**,
**CLASSIFIER**, CLASS_ORIGIN,
**CLOB**,
**CLOSE**,
**COALESCE**, COBOL,
**COLLATE**, COLLATION, COLLATION_CATALOG, COLLATION_NAME, COLLATION_SCHEMA,
**COLLECT**,
**COLUMN**, COLUMN_NAME, COMMAND_FUNCTION, COMMAND_FUNCTION_CODE,
**COMMIT**, COMMITTED,
**CONDITION**, CONDITIONAL, CONDITION_NUMBER, CONFIG,
**CONNECT**, CONNECTION, CONNECTION_NAME,
**CONSTRAINT**, CONSTRAINTS, CONSTRAINT_CATALOG, CONSTRAINT_NAME, CONSTRAINT_SCHEMA, CONSTRUCTOR,
**CONTAINS**, CONTINUE,
**CONVERT**,
**CORR**,
**CORRESPONDING**,
**COUNT**,
**COVAR_POP**,
**COVAR_SAMP**,
**CREATE**,
**CROSS**,
**CUBE**,
**CUME_DIST**,
**CURRENT**,
**CURRENT_CATALOG**,
**CURRENT_DATE**,
**CURRENT_DEFAULT_TRANSFORM_GROUP**,
**CURRENT_PATH**,
**CURRENT_ROLE**,
**CURRENT_ROW**,
**CURRENT_SCHEMA**,
**CURRENT_TIME**,
**CURRENT_TIMESTAMP**,
**CURRENT_TRANSFORM_GROUP_FOR_TYPE**,
**CURRENT_USER**,
**CURSOR**, CURSOR_NAME,
**CYCLE**, DATA, DATABASE,
**DATE**, DATETIME_INTERVAL_CODE, DATETIME_INTERVAL_PRECISION,
**DAY**,
**DEALLOCATE**,
**DEC**, DECADE,
**DECIMAL**,
**DECLARE**,
**DEFAULT**, DEFAULTS, DEFERRABLE, DEFERRED,
**DEFINE**, DEFINED, DEFINER, DEGREE,
**DELETE**,
**DENSE_RANK**, DEPTH,
**DEREF**, DERIVED, DESC,
**DESCRIBE**, DESCRIPTION, DESCRIPTOR,
**DETERMINISTIC**, DIAGNOSTICS,
**DISALLOW**,
**DISCONNECT**, DISPATCH,
**DISTANCE**,
**DISTINCT**, DOMAIN,
**DOUBLE**, DOW, DOY,
**DROP**,
**DYNAMIC**, DYNAMIC_FUNCTION, DYNAMIC_FUNCTION_CODE,
**EACH**,
**ELEMENT**,
**ELSE**,
**EMPTY**, ENCODING,
**END**,
**END-EXEC**,
**END_FRAME**,
**END_PARTITION**, EPOCH,
**EQUALS**, ERROR,
**ESCAPE**,
**EVERY**,
**EXCEPT**, EXCEPTION, EXCLUDE, EXCLUDING,
**EXEC**,
**EXECUTE**,
**EXISTS**,
**EXP**,
**EXPLAIN**,
**EXTEND**,
**EXTERNAL**,
**EXTRACT**,
**FALSE**,
**FETCH**,
**FILE**,
**FILTER**, FINAL, FIRST,
**FIRST_VALUE**,
**FLOAT**,
**FLOOR**, FOLLOWING,
**FOR**,
**FOREIGN**, FORMAT, FORTRAN, FOUND, FRAC_SECOND,
**FRAME_ROW**,
**FREE**,
**FREQUENCY**,
**FROM**,
**FULL**,
**FUNCTION**,
**FUSION**, G, GENERAL, GENERATED, GEOMETRY,
**GET**,
**GLOBAL**, GO, GOTO,
**GRANT**, GRANTED,
**GROUP**,
**GROUPING**,
**GROUPS**,
**HAVING**, HIERARCHY,
**HOLD**,
**HOUR**,
**IDENTITY**,
**IF**,
**IMAGE**, IMMEDIATE, IMMEDIATELY, IMPLEMENTATION,
**IMPORT**,
**IN**, INCLUDING, INCREMENT, INDEX,
**INDICATOR**,
**INITIAL**, INITIALLY,
**INNER**,
**INOUT**, INPUT,
**INSENSITIVE**,
**INSERT**, INSTANCE, INSTANTIABLE,
**INT**,
**INTEGER**, INTERFACES,
**INTERSECT**,
**INTERSECTION**,
**INTERVAL**,
**INTO**, INVOKER,
**IS**, ISODOW, ISOLATION, ISOYEAR,
**JAR**, JAVA,
**JOIN**, JSON,
**JSON_ARRAY**,
**JSON_ARRAYAGG**,
**JSON_EXISTS**,
**JSON_OBJECT**,
**JSON_OBJECTAGG**,
**JSON_QUERY**,
**JSON_VALUE**, K, KEY, KEY_MEMBER, KEY_TYPE, LABEL,
**LAG**,
**LANGUAGE**,
**LARGE**, LAST,
**LAST_VALUE**,
**LATERAL**,
**LEAD**,
**LEADING**,
**LEFT**, LENGTH, LEVEL, LIBRARY,
**LIKE**,
**LIKE_REGEX**,
**LIMIT**,
**LN**,
**LOCAL**,
**LOCALTIME**,
**LOCALTIMESTAMP**, LOCATOR,
**LOWER**, M, MAP,
**MATCH**, MATCHED,
**MATCHES**,
**MATCH_NUMBER**,
**MATCH_RECOGNIZE**,
**MAX**, MAXVALUE,
**MEASURES**,
**MEMBER**,
**MERGE**, MESSAGE_LENGTH, MESSAGE_OCTET_LENGTH, MESSAGE_TEXT, META,
**METHOD**, MICROSECOND, MILLENNIUM, MILLISECOND,
**MIN**,
**MINUS**,
**MINUTE**, MINVALUE,
**MOD**,
**MODIFIES**, MODIFY,
**MODULE**,
**MONTH**, MORE,
**MULTISET**, MUMPS, NAME, NAMES, NANOSECOND,
**NATIONAL**,
**NATURAL**,
**NCHAR**,
**NCLOB**, NESTING,
**NEW**,
**NEXT**,
**NO**,
**NONE**,
**NORMALIZE**, NORMALIZED,
**NOT**,
**NTH_VALUE**,
**NTILE**,
**NULL**, NULLABLE,
**NULLIF**, NULLS, NUMBER,
**NUMERIC**, OBJECT,
**OCCURRENCES_REGEX**, OCTETS,
**OCTET_LENGTH**,
**OF**,
**OFFSET**,
**OLD**,
**OMIT**,
**ON**,
**ONE**,
**ONLY**,
**OPEN**, OPTION, OPTIONS,
**OR**,
**ORDER**, ORDERING, ORDINALITY, OTHERS,
**OUT**,
**OUTER**, OUTPUT,
**OVER**,
**OVERLAPS**,
**OVERLAY**, OVERRIDING, OWNER, PAD,
**PARAMETER**, PARAMETER_MODE, PARAMETER_NAME, PARAMETER_ORDINAL_POSITION, PARAMETER_SPECIFIC_CATALOG, PARAMETER_SPECIFIC_NAME, PARAMETER_SPECIFIC_SCHEMA, PARTIAL,
**PARTITION**,
**PARTITIONS**, PASCAL, PASSING, PASSTHROUGH, PAST, PATH,
**PATTERN**,
**PER**,
**PERCENT**,
**PERCENTILE_CONT**,
**PERCENTILE_DISC**,
**PERCENT_RANK**,
**PERIOD**,
**PERMUTE**, PLACEMENT, PLACING, PLAN, PLI,
**PORTION**,
**POSITION**,
**POSITION_REGEX**,
**POWER**,
**PRECEDES**, PRECEDING,
**PRECISION**,
**PREPARE**, PRESERVE,
**PREV**,
**PRIMARY**, PRIOR, PRIVILEGES,
**PROCEDURE**, PUBLIC, QUARTER,
**RANGE**,
**RANK**, READ,
**READS**,
**REAL**,
**RECURSIVE**,
**REF**,
**REFERENCES**,
**REFERENCING**,
**REGR_AVGX**,
**REGR_AVGY**,
**REGR_COUNT**,
**REGR_INTERCEPT**,
**REGR_R2**,
**REGR_SLOPE**,
**REGR_SXX**,
**REGR_SXY**,
**REGR_SYY**, RELATIVE,
**RELEASE**, RENAME, REPEATABLE, REPLACE,
**RESET**, RESTART, RESTRICT,
**RESULT**,
**RETURN**, RETURNED_CARDINALITY, RETURNED_LENGTH, RETURNED_OCTET_LENGTH, RETURNED_SQLSTATE, RETURNING,
**RETURNS**,
**REVOKE**,
**RIGHT**, ROLE,
**ROLLBACK**,
**ROLLUP**, ROUTINE, ROUTINE_CATALOG, ROUTINE_NAME, ROUTINE_SCHEMA,
**ROW**,
**ROWS**, ROW_COUNT,
**ROW_NUMBER**,
**RUNNING**,
**SAVEPOINT**, SCALAR, SCALE, SCHEMA, SCHEMA_NAME,
**SCOPE**, SCOPE_CATALOGS, SCOPE_NAME, SCOPE_SCHEMA,
**SCROLL**,
**SEARCH**,
**SECOND**, SECTION, SECURITY,
**SEEK**,
**SELECT**, SELF,
**SENSITIVE**, SEQUENCE, SERIALIZABLE, SERVER, SERVER_NAME, SESSION,
**SESSION_USER**,
**SET**, SETS,
**SHOW**,
**SIMILAR**, SIMPLE, SIZE,
**SKIP**,
**SMALLINT**,
**SOME**,
**SOUND**, SOURCE, SPACE,
**SPECIFIC**,
**SPECIFICTYPE**, SPECIFIC_NAME,
**SQL**,
**SQLEXCEPTION**,
**SQLSTATE**,
**SQLWARNING**, SQL_BIGINT, SQL_BINARY, SQL_BIT, SQL_BLOB, SQL_BOOLEAN, SQL_CHAR, SQL_CLOB, SQL_DATE, SQL_DECIMAL, SQL_DOUBLE, SQL_FLOAT, SQL_INTEGER, SQL_INTERVAL_DAY, SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL_DAY_TO_MINUTE, SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_HOUR, SQL_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL_MINUTE, SQL_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL_MONTH, SQL_INTERVAL_SECOND, SQL_INTERVAL_YEAR, SQL_INTERVAL_YEAR_TO_MONTH, SQL_LONGVARBINARY, SQL_LONGVARCHAR, SQL_LONGVARNCHAR, SQL_NCHAR, SQL_NCLOB, SQL_NUMERIC, SQL_NVARCHAR, SQL_REAL, SQL_SMALLINT, SQL_TIME, SQL_TIMESTAMP, SQL_TINYINT, SQL_TSI_DAY, SQL_TSI_FRAC_SECOND, SQL_TSI_HOUR, SQL_TSI_MICROSECOND, SQL_TSI_MINUTE, SQL_TSI_MONTH, SQL_TSI_QUARTER, SQL_TSI_SECOND, SQL_TSI_WEEK, SQL_TSI_YEAR, SQL_VARBINARY, SQL_VARCHAR,
**SQRT**,
**START**, STATE, STATEMENT,
**STATIC**,
**STDDEV_POP**,
**STDDEV_SAMP**, STORE,
**STORED**,
**STREAM**, STRUCTURE, STYLE, SUBCLASS_ORIGIN,
**SUBMULTISET**,
**SUBSET**, SUBSTITUTE,
**SUBSTRING**,
**SUBSTRING_REGEX**,
**SUCCEEDS**,
**SUM**,
**SYMMETRIC**,
**SYSTEM**,
**SYSTEM_TIME**,
**SYSTEM_USER**,
**TABLE**,
**TABLESAMPLE**,
TABLE_NAME,
**TEMPERATURE**,
TEMPORARY,
**THEN**,
TIES,
**TABLESAMPLE**, TABLE_NAME, TEMPORARY,
**THEN**, TIES,
**TIME**,
**TIMESTAMP**, TIMESTAMPADD, TIMESTAMPDIFF,
**TIMEZONE_HOUR**,
**TIMEZONE_MINUTE**,
**TINYINT**,
**TO**, TOP_LEVEL_COUNT,
**TRAILING**, TRANSACTION, TRANSACTIONS_ACTIVE, TRANSACTIONS_COMMITTED, TRANSACTIONS_ROLLED_BACK, TRANSFORM, TRANSFORMS,
**TRANSLATE**,
**TRANSLATE_REGEX**,
**TRANSLATION**,
**TREAT**,
**TRIGGER**, TRIGGER_CATALOG, TRIGGER_NAME, TRIGGER_SCHEMA,
**TRIM**,
**TRIM_ARRAY**,
**TRUE**,
**TRUNCATE**, TYPE,
**UESCAPE**, UNBOUNDED, UNCOMMITTED, UNCONDITIONAL, UNDER,
**UNION**,
**UNIQUE**,
**UNKNOWN**, UNNAMED,
**UNNEST**,
**UPDATE**,
**UPPER**,
**UPSERT**, USAGE,
**USER**, USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_CODE, USER_DEFINED_TYPE_NAME, USER_DEFINED_TYPE_SCHEMA,
**USING**, UTF16, UTF32, UTF8,
**VALUE**,
**VALUES**,
**VALUE_OF**,
**VARBINARY**,
**VARCHAR**,
**VARYING**,
**VAR_POP**,
**VAR_SAMP**, VERSION,
**VERSIONING**,
**VIDEO**, VIEW,
**VIRTUAL**, WEEK,
**WHEN**,
**WHENEVER**,
**WHERE**,
**WIDTH_BUCKET**,
**WINDOW**,
**WITH**,
**WITHIN**,
**WITHOUT**, WORK, WRAPPER, WRITE, XML,
**YEAR**, ZONE. {% comment %} end {% endcomment %}
