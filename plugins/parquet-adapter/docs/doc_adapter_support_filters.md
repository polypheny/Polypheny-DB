# Document Adapter Filter Pushdown Changeset

Document Parquet scans now support filter pushdown by translating supported document predicates into `ParquetAdapterFilter` instances stored directly on `ParquetDocScan`.
For MQL document queries, Polypheny lowers field access in filters to `MQL_QUERY_VALUE(document, ARRAY['field'])`; the translator now recognizes this top-level field access form and can push supported comparison predicates with literals or dynamic parameters into the Parquet scan.
The implementation moves document filtering from a custom filter node/rule to an `EnumerableCalc`-based planner rule, while keeping the Calc as a residual correctness check.

## Changed Files

## ParquetPlugin.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/ParquetPlugin.java` 
Registers `ParquetDocScan` as `P_DOC_SCAN` in PolyAlg with fields and filters display parameters.


## ParquetDocFilterTranslator.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/execution/ParquetDocFilterTranslator.java`
Extends document filter translation to support lowered `MQL_QUERY_VALUE` top-level field access in addition to simple name references.


## ParquetDocCalcRule.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/planning/ParquetDocCalcRule.java`
Adds a planner rule that pushes supported `EnumerableCalc` conditions into `ParquetDocScan` filters.


## ParquetDocFilter.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/planning/ParquetDocFilter.java`
Removes the obsolete Parquet-specific document filter node.


## ParquetDocFilterRule.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/planning/ParquetDocFilterRule.java`
Removes the obsolete logical filter rewrite rule replaced by calc-based pushdown.


## ParquetDocScan.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/document/planning/ParquetDocScan.java`
Stores typed pushed filters, combines additional filters, exposes them in PolyAlg, adjusts filtered scan cost, and registers the new calc rule.


## ParquetRelAggregate.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/planning/ParquetRelAggregate.java`
Switches PolyAlg display support to the shared Parquet display helper.


## ParquetRelJoin.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/planning/ParquetRelJoin.java`
Switches PolyAlg display support to the shared Parquet display helper.


## ParquetRelScan.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/relational/planning/ParquetRelScan.java`
Switches PolyAlg display support to the shared Parquet display helper.


## ParquetPolyAlgDisplay.java

`plugins/parquet-adapter/src/main/java/org/polypheny/db/adapter/parquet/shared/planning/ParquetPolyAlgDisplay.java`
Adds a shared helper for formatting Parquet fields and filters in PolyAlg output.


## ParquetDocumentPlanningTest.java

`plugins/parquet-adapter/src/test/java/org/polypheny/db/adapter/parquet/ParquetDocumentPlanningTest.java`
Updates document planning tests for scan-carried filters and removes tests for the deleted filter node/rule.


## ParquetSharedDocumentAdapterTest.java

`plugins/parquet-adapter/src/test/java/org/polypheny/db/adapter/parquet/ParquetSharedDocumentAdapterTest.java`
Adds translator coverage for comparison filters and lowered MQL field access.
