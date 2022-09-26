/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.util;


import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.CorrelateJoinType;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.ExtendedEnumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.AggregateLambdaFactory;
import org.polypheny.db.adapter.enumerable.BatchIteratorEnumerable;
import org.polypheny.db.adapter.enumerable.OrderedAggregateLambdaFactory;
import org.polypheny.db.adapter.enumerable.SequencedAdderAggregateLambdaFactory;
import org.polypheny.db.adapter.enumerable.SourceSorter;
import org.polypheny.db.adapter.enumerable.lpg.EnumerableLpgMatch.MatchEnumerable;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.json.JsonConstructorNullClause;
import org.polypheny.db.algebra.json.JsonQueryEmptyOrErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryWrapperBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.AllPredicates;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Collation;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.ColumnOrigin;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.ColumnUniqueness;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.CumulativeCost;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.DistinctRowCount;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Distribution;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.ExplainVisibility;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.ExpressionLineage;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.MaxRowCount;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Memory;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.MinRowCount;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.NodeTypes;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.NonCumulativeCost;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Parallelism;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.PercentageOriginalRows;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.PopulationSize;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Predicates;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.RowCount;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Selectivity;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Size;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.TableReferences;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.UniqueKeys;
import org.polypheny.db.algebra.metadata.Metadata;
import org.polypheny.db.interpreter.Context;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.interpreter.Scalar;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.ArrayBindable;
import org.polypheny.db.runtime.BinarySearch;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Enumerables;
import org.polypheny.db.runtime.FlatLists;
import org.polypheny.db.runtime.RandomFunction;
import org.polypheny.db.runtime.SortedMultiMap;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.runtime.functions.CrossModelFunctions;
import org.polypheny.db.runtime.functions.CypherFunctions;
import org.polypheny.db.runtime.functions.Functions;
import org.polypheny.db.runtime.functions.Functions.FlatProductInputType;
import org.polypheny.db.runtime.functions.MqlFunctions;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.graph.GraphPropertyHolder;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyType;


/**
 * Built-in methods.
 */
public enum BuiltInMethod {
    SWITCH_CONTEXT( DataContext.class, "switchContext" ),
    BATCH( Functions.class, "batch", DataContext.class, Enumerable.class ),
    STREAM_RIGHT( Functions.class, "streamRight", DataContext.class, Enumerable.class, Function0.class, List.class ),
    ENFORCE_CONSTRAINT( Functions.class, "enforceConstraint", Function0.class, Enumerable.class, List.class, List.class ),
    TO_NODE( CypherFunctions.class, "toNode", Enumerable.class ),
    TO_EDGE( CypherFunctions.class, "toEdge", Enumerable.class ),
    TO_GRAPH( CypherFunctions.class, "toGraph", Enumerable.class, Enumerable.class ),
    PARSE_ARRAY_FROM_TEXT( Functions.class, "reparse", PolyType.class, Long.class, String.class ),
    QUERYABLE_SELECT( Queryable.class, "select", FunctionExpression.class ),
    QUERYABLE_AS_ENUMERABLE( Queryable.class, "asEnumerable" ),
    QUERYABLE_TABLE_AS_QUERYABLE( QueryableTable.class, "asQueryable", DataContext.class, SchemaPlus.class, String.class ),
    AS_QUERYABLE( Enumerable.class, "asQueryable" ),
    ABSTRACT_ENUMERABLE_CTOR( AbstractEnumerable.class ),
    BATCH_ITERATOR_CTOR( BatchIteratorEnumerable.class ),
    BATCH_ITERATOR_GET_ENUM( BatchIteratorEnumerable.class, "getEnumerable" ),
    INTO( ExtendedEnumerable.class, "into", Collection.class ),
    REMOVE_ALL( ExtendedEnumerable.class, "removeAll", Collection.class ),
    SCHEMA_GET_SUB_SCHEMA( Schema.class, "getSubSchema", String.class ),
    SCHEMA_GET_TABLE( Schema.class, "getTable", String.class ),
    SCHEMA_PLUS_UNWRAP( SchemaPlus.class, "unwrap", Class.class ),
    SCHEMAS_ENUMERABLE_SCANNABLE( Schemas.class, "enumerable", ScannableTable.class, DataContext.class ),
    SCHEMAS_ENUMERABLE_FILTERABLE( Schemas.class, "enumerable", FilterableTable.class, DataContext.class ),
    SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE( Schemas.class, "enumerable", ProjectableFilterableTable.class, DataContext.class ),
    SCHEMAS_QUERYABLE( Schemas.class, "queryable", DataContext.class, SchemaPlus.class, Class.class, String.class ),
    REFLECTIVE_SCHEMA_GET_TARGET( ReflectiveSchema.class, "getTarget" ),
    DATA_CONTEXT_GET( DataContext.class, "get", String.class ),
    DATA_CONTEXT_GET_PARAMETER_VALUE( DataContext.class, "getParameterValue", long.class ),
    DATA_CONTEXT_GET_ROOT_SCHEMA( DataContext.class, "getRootSchema" ),
    //JDBC_SCHEMA_DATA_SOURCE( JdbcSchema.class, "getDataSource" ),
    ROW_VALUE( Row.class, "getObject", int.class ),
    ROW_AS_COPY( Row.class, "asCopy", Object[].class ),
    JOIN( ExtendedEnumerable.class, "join", Enumerable.class, Function1.class, Function1.class, Function2.class ),
    MERGE_JOIN( EnumerableDefaults.class, "mergeJoin", Enumerable.class, Enumerable.class, Function1.class, Function1.class, Function2.class, boolean.class, boolean.class ),
    SLICE0( Enumerables.class, "slice0", Enumerable.class ),
    SEMI_JOIN( EnumerableDefaults.class, "semiJoin", Enumerable.class, Enumerable.class, Function1.class, Function1.class ),
    THETA_JOIN( EnumerableDefaults.class, "thetaJoin", Enumerable.class, Enumerable.class, Predicate2.class, Function2.class, boolean.class, boolean.class ),
    SINGLE_SUM( Functions.class, "singleSum", Enumerable.class ),
    CORRELATE_JOIN( ExtendedEnumerable.class, "correlateJoin", CorrelateJoinType.class, Function1.class, Function2.class ),
    SELECT( ExtendedEnumerable.class, "select", Function1.class ),
    SELECT2( ExtendedEnumerable.class, "select", Function2.class ),
    SELECT_MANY( ExtendedEnumerable.class, "selectMany", Function1.class ),
    WHERE( ExtendedEnumerable.class, "where", Predicate1.class ),
    WHERE2( ExtendedEnumerable.class, "where", Predicate2.class ),
    DISTINCT( ExtendedEnumerable.class, "distinct" ),
    DISTINCT2( ExtendedEnumerable.class, "distinct", EqualityComparer.class ),
    GROUP_BY( ExtendedEnumerable.class, "groupBy", Function1.class ),
    GROUP_BY2( ExtendedEnumerable.class, "groupBy", Function1.class, Function0.class, Function2.class, Function2.class ),
    GROUP_BY_MULTIPLE( EnumerableDefaults.class, "groupByMultiple", Enumerable.class, List.class, Function0.class, Function2.class, Function2.class ),
    AGGREGATE( ExtendedEnumerable.class, "aggregate", Object.class, Function2.class, Function1.class ),
    ORDER_BY( ExtendedEnumerable.class, "orderBy", Function1.class, Comparator.class ),
    UNION( ExtendedEnumerable.class, "union", Enumerable.class ),
    CONCAT( ExtendedEnumerable.class, "concat", Enumerable.class ),
    INTERSECT( ExtendedEnumerable.class, "intersect", Enumerable.class ),
    EXCEPT( ExtendedEnumerable.class, "except", Enumerable.class ),
    SKIP( ExtendedEnumerable.class, "skip", int.class ),
    TAKE( ExtendedEnumerable.class, "take", int.class ),
    SINGLETON_ENUMERABLE( Linq4j.class, "singletonEnumerable", Object.class ),
    EMPTY_ENUMERABLE( Linq4j.class, "emptyEnumerable" ),
    NULLS_COMPARATOR( org.apache.calcite.linq4j.function.Functions.class, "nullsComparator", boolean.class, boolean.class ),
    ARRAY_COMPARER( org.apache.calcite.linq4j.function.Functions.class, "arrayComparer" ),
    FUNCTION0_APPLY( Function0.class, "apply" ),
    FUNCTION1_APPLY( Function1.class, "apply", Object.class ),
    ARRAYS_AS_LIST( Arrays.class, "asList", Object[].class ),
    MAP_OF_ENTRIES( ImmutableMap.class, "copyOf", List.class ),
    ARRAY( Functions.class, "array", Object[].class ),
    FLAT_PRODUCT( Functions.class, "flatProduct", int[].class, boolean.class, FlatProductInputType[].class ),
    LIST_N( FlatLists.class, "copyOf", Comparable[].class ),
    LIST2( FlatLists.class, "of", Object.class, Object.class ),
    LIST3( FlatLists.class, "of", Object.class, Object.class, Object.class ),
    LIST4( FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class ),
    LIST5( FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class, Object.class ),
    LIST6( FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class, Object.class, Object.class ),
    COMPARABLE_EMPTY_LIST( FlatLists.class, "COMPARABLE_EMPTY_LIST", true ),
    IDENTITY_COMPARER( org.apache.calcite.linq4j.function.Functions.class, "identityComparer" ),
    IDENTITY_SELECTOR( org.apache.calcite.linq4j.function.Functions.class, "identitySelector" ),
    AS_ENUMERABLE( Linq4j.class, "asEnumerable", Object[].class ),
    AS_ENUMERABLE2( Linq4j.class, "asEnumerable", Iterable.class ),
    ENUMERABLE_TO_LIST( ExtendedEnumerable.class, "toList" ),
    AS_LIST( Primitive.class, "asList", Object.class ),
    PAIR_OF( Pair.class, "of", Pair.class ),
    ENUMERATOR_CURRENT( Enumerator.class, "current" ),
    ENUMERATOR_MOVE_NEXT( Enumerator.class, "moveNext" ),
    ENUMERATOR_CLOSE( Enumerator.class, "close" ),
    ENUMERATOR_RESET( Enumerator.class, "reset" ),
    ENUMERABLE_ENUMERATOR( Enumerable.class, "enumerator" ),
    ENUMERABLE_FOREACH( Enumerable.class, "foreach", Function1.class ),
    TYPED_GET_ELEMENT_TYPE( ArrayBindable.class, "getElementType" ),
    BINDABLE_BIND( Bindable.class, "bind", DataContext.class ),
    RESULT_SET_GET_DATE2( ResultSet.class, "getDate", int.class, Calendar.class ),
    RESULT_SET_GET_TIME2( ResultSet.class, "getTime", int.class, Calendar.class ),
    RESULT_SET_GET_TIMESTAMP2( ResultSet.class, "getTimestamp", int.class, Calendar.class ),
    TIME_ZONE_GET_OFFSET( TimeZone.class, "getOffset", long.class ),
    LONG_VALUE( Number.class, "longValue" ),
    COMPARATOR_COMPARE( Comparator.class, "compare", Object.class, Object.class ),
    COLLECTIONS_REVERSE_ORDER( Collections.class, "reverseOrder" ),
    COLLECTIONS_EMPTY_LIST( Collections.class, "emptyList" ),
    COLLECTIONS_SINGLETON_LIST( Collections.class, "singletonList", Object.class ),
    COLLECTION_SIZE( Collection.class, "size" ),
    MAP_CLEAR( Map.class, "clear" ),
    MAP_GET( Map.class, "get", Object.class ),
    MAP_PUT( Map.class, "put", Object.class, Object.class ),
    COLLECTION_ADD( Collection.class, "add", Object.class ),
    COLLECTION_ADDALL( Collection.class, "addAll", Collection.class ),
    LIST_GET( List.class, "get", int.class ),
    ITERATOR_HAS_NEXT( Iterator.class, "hasNext" ),
    ITERATOR_NEXT( Iterator.class, "next" ),
    MATH_MAX( Math.class, "max", int.class, int.class ),
    MATH_MIN( Math.class, "min", int.class, int.class ),
    SORTED_MULTI_MAP_PUT_MULTI( SortedMultiMap.class, "putMulti", Object.class, Object.class ),
    SORTED_MULTI_MAP_ARRAYS( SortedMultiMap.class, "arrays", Comparator.class ),
    SORTED_MULTI_MAP_SINGLETON( SortedMultiMap.class, "singletonArrayIterator", Comparator.class, List.class ),
    BINARY_SEARCH5_LOWER( BinarySearch.class, "lowerBound", Object[].class, Object.class, int.class, int.class, Comparator.class ),
    BINARY_SEARCH5_UPPER( BinarySearch.class, "upperBound", Object[].class, Object.class, int.class, int.class, Comparator.class ),
    BINARY_SEARCH6_LOWER( BinarySearch.class, "lowerBound", Object[].class, Object.class, int.class, int.class, Function1.class, Comparator.class ),
    BINARY_SEARCH6_UPPER( BinarySearch.class, "upperBound", Object[].class, Object.class, int.class, int.class, Function1.class, Comparator.class ),
    ARRAY_ITEM( Functions.class, "arrayItemOptional", List.class, int.class ),
    MAP_ITEM( Functions.class, "mapItemOptional", Map.class, Object.class ),
    ANY_ITEM( Functions.class, "itemOptional", Object.class, Object.class ),
    UPPER( Functions.class, "upper", String.class ),
    LOWER( Functions.class, "lower", String.class ),
    JSONIZE( Functions.class, "jsonize", Object.class ),
    JSON_VALUE_EXPRESSION( Functions.class, "jsonValueExpression", String.class ),
    JSON_VALUE_EXPRESSION_EXCLUDE( Functions.class, "jsonValueExpressionExclude", String.class, List.class ),
    JSON_STRUCTURED_VALUE_EXPRESSION( Functions.class, "jsonStructuredValueExpression", Object.class ),
    JSON_API_COMMON_SYNTAX( Functions.class, "jsonApiCommonSyntax", Object.class, String.class ),
    JSON_EXISTS( Functions.class, "jsonExists", Object.class ),
    JSON_VALUE_ANY( Functions.class, "jsonValueAny", Object.class, JsonValueEmptyOrErrorBehavior.class, Object.class, JsonValueEmptyOrErrorBehavior.class, Object.class ),
    JSON_QUERY( Functions.class, "jsonQuery", Object.class, JsonQueryWrapperBehavior.class, JsonQueryEmptyOrErrorBehavior.class, JsonQueryEmptyOrErrorBehavior.class ),
    JSON_OBJECT( Functions.class, "jsonObject", JsonConstructorNullClause.class ),
    JSON_OBJECTAGG_ADD( Functions.class, "jsonObjectAggAdd", Map.class, String.class, Object.class, JsonConstructorNullClause.class ),
    JSON_ARRAY( Functions.class, "jsonArray", JsonConstructorNullClause.class ),
    JSON_ARRAYAGG_ADD( Functions.class, "jsonArrayAggAdd", List.class, Object.class, JsonConstructorNullClause.class ),
    IS_JSON_VALUE( Functions.class, "isJsonValue", String.class ),
    IS_JSON_OBJECT( Functions.class, "isJsonObject", String.class ),
    IS_JSON_ARRAY( Functions.class, "isJsonArray", String.class ),
    IS_JSON_SCALAR( Functions.class, "isJsonScalar", String.class ),
    INITCAP( Functions.class, "initcap", String.class ),
    SUBSTRING( Functions.class, "substring", String.class, int.class, int.class ),
    CHAR_LENGTH( Functions.class, "charLength", String.class ),
    STRING_CONCAT( Functions.class, "concat", String.class, String.class ),
    FLOOR_DIV( DateTimeUtils.class, "floorDiv", long.class, long.class ),
    FLOOR_MOD( DateTimeUtils.class, "floorMod", long.class, long.class ),
    ADD_MONTHS( Functions.class, "addMonths", long.class, int.class ),
    ADD_MONTHS_INT( Functions.class, "addMonths", int.class, int.class ),
    SUBTRACT_MONTHS( Functions.class, "subtractMonths", long.class, long.class ),
    FLOOR( Functions.class, "floor", int.class, int.class ),
    CEIL( Functions.class, "ceil", int.class, int.class ),
    OVERLAY( Functions.class, "overlay", String.class, String.class, int.class ),
    OVERLAY3( Functions.class, "overlay", String.class, String.class, int.class, int.class ),
    POSITION( Functions.class, "position", String.class, String.class ),
    RAND( RandomFunction.class, "rand" ),
    RAND_SEED( RandomFunction.class, "randSeed", int.class ),
    RAND_INTEGER( RandomFunction.class, "randInteger", int.class ),
    RAND_INTEGER_SEED( RandomFunction.class, "randIntegerSeed", int.class, int.class ),
    TRUNCATE( Functions.class, "truncate", String.class, int.class ),
    TRUNCATE_OR_PAD( Functions.class, "truncateOrPad", String.class, int.class ),
    TRIM( Functions.class, "trim", boolean.class, boolean.class, String.class, String.class, boolean.class ),
    REPLACE( Functions.class, "replace", String.class, String.class, String.class ),
    TRANSLATE3( Functions.class, "translate3", String.class, String.class, String.class ),
    LTRIM( Functions.class, "ltrim", String.class ),
    RTRIM( Functions.class, "rtrim", String.class ),
    LIKE( Functions.class, "like", String.class, String.class ),
    SIMILAR( Functions.class, "similar", String.class, String.class ),
    IS_TRUE( Functions.class, "isTrue", Boolean.class ),
    IS_NOT_FALSE( Functions.class, "isNotFalse", Boolean.class ),
    NOT( Functions.class, "not", Boolean.class ),
    LESSER( Functions.class, "lesser", Comparable.class, Comparable.class ),
    GREATER( Functions.class, "greater", Comparable.class, Comparable.class ),
    BIT_AND( Functions.class, "bitAnd", long.class, long.class ),
    BIT_OR( Functions.class, "bitOr", long.class, long.class ),
    MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION( ModifiableTable.class, "getModifiableCollection" ),
    SCANNABLE_TABLE_SCAN( ScannableTable.class, "scan", DataContext.class ),
    STRING_TO_BOOLEAN( Functions.class, "toBoolean", String.class ),
    INTERNAL_TO_DATE( Functions.class, "internalToDate", int.class ),
    INTERNAL_TO_TIME( Functions.class, "internalToTime", int.class ),
    INTERNAL_TO_TIMESTAMP( Functions.class, "internalToTimestamp", long.class ),
    STRING_TO_DATE( DateTimeUtils.class, "dateStringToUnixDate", String.class ),
    STRING_TO_TIME( DateTimeUtils.class, "timeStringToUnixDate", String.class ),
    STRING_TO_TIMESTAMP( DateTimeUtils.class, "timestampStringToUnixDate", String.class ),
    STRING_TO_TIME_WITH_LOCAL_TIME_ZONE( Functions.class, "toTimeWithLocalTimeZone", String.class ),
    TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE( Functions.class, "toTimeWithLocalTimeZone", String.class, TimeZone.class ),
    STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE( Functions.class, "toTimestampWithLocalTimeZone", String.class ),
    TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE( Functions.class, "toTimestampWithLocalTimeZone", String.class, TimeZone.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIME( Functions.class, "timeWithLocalTimeZoneToTime", int.class, TimeZone.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP( Functions.class, "timeWithLocalTimeZoneToTimestamp", String.class, int.class, TimeZone.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE( Functions.class, "timeWithLocalTimeZoneToTimestampWithLocalTimeZone", String.class, int.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_STRING( Functions.class, "timeWithLocalTimeZoneToString", int.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE( Functions.class, "timestampWithLocalTimeZoneToDate", long.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME( Functions.class, "timestampWithLocalTimeZoneToTime", long.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE( Functions.class, "timestampWithLocalTimeZoneToTimeWithLocalTimeZone", long.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP( Functions.class, "timestampWithLocalTimeZoneToTimestamp", long.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING( Functions.class, "timestampWithLocalTimeZoneToString", long.class, TimeZone.class ),
    UNIX_DATE_TO_STRING( DateTimeUtils.class, "unixDateToString", int.class ),
    UNIX_TIME_TO_STRING( DateTimeUtils.class, "unixTimeToString", int.class ),
    UNIX_TIMESTAMP_TO_STRING( DateTimeUtils.class, "unixTimestampToString", long.class ),
    INTERVAL_YEAR_MONTH_TO_STRING( DateTimeUtils.class, "intervalYearMonthToString", int.class, TimeUnitRange.class ),
    INTERVAL_DAY_TIME_TO_STRING( DateTimeUtils.class, "intervalDayTimeToString", long.class, TimeUnitRange.class, int.class ),
    UNIX_DATE_EXTRACT( DateTimeUtils.class, "unixDateExtract", TimeUnitRange.class, long.class ),
    UNIX_DATE_FLOOR( DateTimeUtils.class, "unixDateFloor", TimeUnitRange.class, int.class ),
    UNIX_DATE_CEIL( DateTimeUtils.class, "unixDateCeil", TimeUnitRange.class, int.class ),
    UNIX_TIMESTAMP_FLOOR( DateTimeUtils.class, "unixTimestampFloor", TimeUnitRange.class, long.class ),
    UNIX_TIMESTAMP_CEIL( DateTimeUtils.class, "unixTimestampCeil", TimeUnitRange.class, long.class ),
    CURRENT_TIMESTAMP( Functions.class, "currentTimestamp", DataContext.class ),
    CURRENT_TIME( Functions.class, "currentTime", DataContext.class ),
    CURRENT_DATE( Functions.class, "currentDate", DataContext.class ),
    LOCAL_TIMESTAMP( Functions.class, "localTimestamp", DataContext.class ),
    LOCAL_TIME( Functions.class, "localTime", DataContext.class ),
    TIME_ZONE( Functions.class, "timeZone", DataContext.class ),
    BOOLEAN_TO_STRING( Functions.class, "toString", boolean.class ),
    JDBC_ARRAY_TO_LIST( Functions.class, "arrayToList", java.sql.Array.class ),
    JDBC_DEEP_ARRAY_TO_LIST( Functions.class, "deepArrayToList", java.sql.Array.class ),
    OBJECT_TO_STRING( Object.class, "toString" ),
    OBJECTS_EQUAL( Objects.class, "equals", Object.class, Object.class ),
    HASH( Utilities.class, "hash", int.class, Object.class ),
    COMPARE( Utilities.class, "compare", Comparable.class, Comparable.class ),
    COMPARE_NULLS_FIRST( Utilities.class, "compareNullsFirst", Comparable.class, Comparable.class ),
    COMPARE_NULLS_LAST( Utilities.class, "compareNullsLast", Comparable.class, Comparable.class ),
    ROUND_LONG( Functions.class, "round", long.class, long.class ),
    ROUND_INT( Functions.class, "round", int.class, int.class ),
    DATE_TO_INT( Functions.class, "toInt", java.util.Date.class ),
    DATE_TO_INT_OPTIONAL( Functions.class, "toIntOptional", java.util.Date.class ),
    TIME_TO_INT( Functions.class, "toInt", Time.class ),
    TIME_TO_INT_OPTIONAL( Functions.class, "toIntOptional", Time.class ),
    TIMESTAMP_TO_LONG( Functions.class, "toLong", java.util.Date.class ),
    TIMESTAMP_TO_LONG_OFFSET( Functions.class, "toLong", java.util.Date.class, TimeZone.class ),
    TIMESTAMP_TO_LONG_OPTIONAL( Functions.class, "toLongOptional", Timestamp.class ),
    TIMESTAMP_TO_LONG_OPTIONAL_OFFSET( Functions.class, "toLongOptional", Timestamp.class, TimeZone.class ),
    SEQUENCE_CURRENT_VALUE( Functions.class, "sequenceCurrentValue", String.class ),
    SEQUENCE_NEXT_VALUE( Functions.class, "sequenceNextValue", String.class ),
    SLICE( Functions.class, "slice", List.class ),
    ELEMENT( Functions.class, "element", List.class ),
    MEMBER_OF( Functions.class, "memberOf", Object.class, Collection.class ),
    MULTISET_INTERSECT_DISTINCT( Functions.class, "multisetIntersectDistinct", Collection.class, Collection.class ),
    MULTISET_INTERSECT_ALL( Functions.class, "multisetIntersectAll", Collection.class, Collection.class ),
    MULTISET_EXCEPT_DISTINCT( Functions.class, "multisetExceptDistinct", Collection.class, Collection.class ),
    MULTISET_EXCEPT_ALL( Functions.class, "multisetExceptAll", Collection.class, Collection.class ),
    MULTISET_UNION_DISTINCT( Functions.class, "multisetUnionDistinct", Collection.class, Collection.class ),
    MULTISET_UNION_ALL( Functions.class, "multisetUnionAll", Collection.class, Collection.class ),
    IS_A_SET( Functions.class, "isASet", Collection.class ),
    IS_EMPTY( Collection.class, "isEmpty" ),
    SUBMULTISET_OF( Functions.class, "submultisetOf", Collection.class, Collection.class ),
    SELECTIVITY( Selectivity.class, "getSelectivity", RexNode.class ),
    UNIQUE_KEYS( UniqueKeys.class, "getUniqueKeys", boolean.class ),
    AVERAGE_ROW_SIZE( Size.class, "averageRowSize" ),
    AVERAGE_COLUMN_SIZES( Size.class, "averageColumnSizes" ),
    IS_PHASE_TRANSITION( Parallelism.class, "isPhaseTransition" ),
    SPLIT_COUNT( Parallelism.class, "splitCount" ),
    MEMORY( Memory.class, "memory" ),
    CUMULATIVE_MEMORY_WITHIN_PHASE( Memory.class, "cumulativeMemoryWithinPhase" ),
    CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT( Memory.class, "cumulativeMemoryWithinPhaseSplit" ),
    COLUMN_UNIQUENESS( ColumnUniqueness.class, "areColumnsUnique", ImmutableBitSet.class, boolean.class ),
    COLLATIONS( Collation.class, "collations" ),
    DISTRIBUTION( Distribution.class, "distribution" ),
    NODE_TYPES( NodeTypes.class, "getNodeTypes" ),
    ROW_COUNT( RowCount.class, "getRowCount" ),
    MAX_ROW_COUNT( MaxRowCount.class, "getMaxRowCount" ),
    MIN_ROW_COUNT( MinRowCount.class, "getMinRowCount" ),
    DISTINCT_ROW_COUNT( DistinctRowCount.class, "getDistinctRowCount", ImmutableBitSet.class, RexNode.class ),
    PERCENTAGE_ORIGINAL_ROWS( PercentageOriginalRows.class, "getPercentageOriginalRows" ),
    POPULATION_SIZE( PopulationSize.class, "getPopulationSize", ImmutableBitSet.class ),
    COLUMN_ORIGIN( ColumnOrigin.class, "getColumnOrigins", int.class ),
    EXPRESSION_LINEAGE( ExpressionLineage.class, "getExpressionLineage", RexNode.class ),
    TABLE_REFERENCES( TableReferences.class, "getTableReferences" ),
    CUMULATIVE_COST( CumulativeCost.class, "getCumulativeCost" ),
    NON_CUMULATIVE_COST( NonCumulativeCost.class, "getNonCumulativeCost" ),
    PREDICATES( Predicates.class, "getPredicates" ),
    ALL_PREDICATES( AllPredicates.class, "getAllPredicates" ),
    EXPLAIN_VISIBILITY( ExplainVisibility.class, "isVisibleInExplain", ExplainLevel.class ),
    SCALAR_EXECUTE1( Scalar.class, "execute", Context.class ),
    SCALAR_EXECUTE2( Scalar.class, "execute", Context.class, Object[].class ),
    CONTEXT_VALUES( Context.class, "values", true ),
    CONTEXT_ROOT( Context.class, "root", true ),
    DATA_CONTEXT_GET_QUERY_PROVIDER( DataContext.class, "getQueryProvider" ),
    METADATA_ALG( Metadata.class, "alg" ),
    STRUCT_ACCESS( Functions.class, "structAccess", Object.class, int.class, String.class ),
    SOURCE_SORTER( SourceSorter.class, Function2.class, Function1.class, Comparator.class ),
    ORDERED_AGGREGATE_LAMBDA_FACTORY( OrderedAggregateLambdaFactory.class, Function0.class, List.class ),
    SEQUENCED_ADDER_AGGREGATE_LAMBDA_FACTORY( SequencedAdderAggregateLambdaFactory.class, Function0.class, List.class ),
    AGG_LAMBDA_FACTORY_ACC_INITIALIZER( AggregateLambdaFactory.class, "accumulatorInitializer" ),
    AGG_LAMBDA_FACTORY_ACC_ADDER( AggregateLambdaFactory.class, "accumulatorAdder" ),
    AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR( AggregateLambdaFactory.class, "resultSelector", Function2.class ),
    AGG_LAMBDA_FACTORY_ACC_SINGLE_GROUP_RESULT_SELECTOR( AggregateLambdaFactory.class, "singleGroupResultSelector", Function1.class ),
    RESULTSET_GETBYTES( ResultSet.class, "getBytes", int.class ),
    RESULTSET_GETBINARYSTREAM( ResultSet.class, "getBinaryStream", int.class ),
    /// MQL BUILT-IN METHODS
    DOC_EQ( MqlFunctions.class, "docEq", Object.class, Object.class ),
    DOC_GT( MqlFunctions.class, "docGt", Object.class, Object.class ),
    DOC_GTE( MqlFunctions.class, "docGte", Object.class, Object.class ),
    DOC_LT( MqlFunctions.class, "docLt", Object.class, Object.class ),
    DOC_LTE( MqlFunctions.class, "docLte", Object.class, Object.class ),
    DOC_SIZE_MATCH( MqlFunctions.class, "docSizeMatch", Object.class, int.class ),
    DOC_JSON_MATCH( MqlFunctions.class, "docJsonMatch", Object.class, String.class ),
    DOC_REGEX_MATCH( MqlFunctions.class, "docRegexMatch", Object.class, String.class, boolean.class, boolean.class, boolean.class, boolean.class ),
    DOC_TYPE_MATCH( MqlFunctions.class, "docTypeMatch", Object.class, List.class ),
    DOC_SLICE( MqlFunctions.class, "docSlice", Object.class, int.class, int.class ),
    DOC_QUERY_VALUE( MqlFunctions.class, "docQueryValue", Object.class, List.class ),
    DOC_QUERY_EXCLUDE( MqlFunctions.class, "docQueryExclude", Object.class, List.class ),
    DOC_ADD_FIELDS( MqlFunctions.class, "docAddFields", Object.class, String.class, Object.class ),
    DOC_UPDATE_MIN( MqlFunctions.class, "docUpdateMin", Object.class, Long.class ),
    DOC_UPDATE_MAX( MqlFunctions.class, "docUpdateMax", Object.class, Long.class ),
    DOC_UPDATE_ADD_TO_SET( MqlFunctions.class, "docAddToSet", Object.class, Object.class ),
    DOC_UPDATE_REMOVE( MqlFunctions.class, "docUpdateRemove", Object.class, List.class ),
    DOC_UPDATE_REPLACE( MqlFunctions.class, "docUpdateReplace", Object.class, List.class, List.class ),
    DOC_UPDATE_RENAME( MqlFunctions.class, "docUpdateRename", Object.class, List.class, List.class ),
    DOC_GET_ARRAY( MqlFunctions.class, "docGetArray", Object.class ),
    DOC_JSONIZE( MqlFunctions.class, "docJsonify", Object.class ),
    DOC_EXISTS( MqlFunctions.class, "docExists", Object.class, List.class ),
    GRAPH_PATH_MATCH( CypherFunctions.class, "pathMatch", PolyGraph.class, PolyPath.class ),
    CYPHER_HAS_LABEL( CypherFunctions.class, "hasLabel", PolyNode.class, String.class ),
    CYPHER_HAS_PROPERTY( CypherFunctions.class, "hasProperty", PolyNode.class, String.class ),
    GRAPH_NODE_MATCH( CypherFunctions.class, "nodeMatch", PolyGraph.class, PolyNode.class ),
    GRAPH_NODE_EXTRACT( CypherFunctions.class, "nodeExtract", PolyGraph.class ),
    GRAPH_MATCH_CTOR( MatchEnumerable.class, List.class ),
    GRAPH_EXTRACT_FROM_PATH( CypherFunctions.class, "extractFrom", PolyPath.class, String.class ),
    CYPHER_EXTRACT_PROPERTY( CypherFunctions.class, "extractProperty", GraphPropertyHolder.class, String.class ),

    CYPHER_EXTRACT_PROPERTIES( CypherFunctions.class, "extractProperties", GraphPropertyHolder.class ),

    CYPHER_EXTRACT_ID( CypherFunctions.class, "extractId", GraphPropertyHolder.class ),
    CYPHER_EXTRACT_LABELS( CypherFunctions.class, "extractLabels", GraphPropertyHolder.class ),
    CYPHER_EXTRACT_LABEL( CypherFunctions.class, "extractLabel", GraphPropertyHolder.class ),
    CYPHER_TO_LIST( CypherFunctions.class, "toList", Object.class ),
    CYPHER_ADJUST_EDGE( CypherFunctions.class, "adjustEdge", PolyEdge.class, PolyNode.class, PolyNode.class ),
    CYPHER_SET_PROPERTY( CypherFunctions.class, "setProperty", GraphPropertyHolder.class, String.class, String.class ),
    CYPHER_SET_PROPERTIES( CypherFunctions.class, "setProperties", GraphPropertyHolder.class, List.class, List.class, boolean.class ),
    CYPHER_SET_LABELS( CypherFunctions.class, "setLabels", GraphPropertyHolder.class, List.class, boolean.class ),
    CYPHER_REMOVE_LABELS( CypherFunctions.class, "removeLabels", GraphPropertyHolder.class, List.class ),
    CYPHER_REMOVE_PROPERTY( CypherFunctions.class, "removeProperty", GraphPropertyHolder.class, String.class ),
    SPLIT_GRAPH_MODIFY( CrossModelFunctions.class, "sendGraphModifies", DataContext.class, List.class, List.class, Operation.class ),

    X_MODEL_TABLE_TO_NODE( CrossModelFunctions.class, "tableToNodes", Enumerable.class, String.class, List.class ),
    X_MODEL_MERGE_NODE_COLLECTIONS( CrossModelFunctions.class, "mergeNodeCollections", List.class ),
    X_MODEL_COLLECTION_TO_NODE( CrossModelFunctions.class, "collectionToNodes", Enumerable.class, String.class ),
    X_MODEL_NODE_TO_COLLECTION( CrossModelFunctions.class, "nodesToCollection", Enumerable.class );

    public final Method method;
    public final Constructor constructor;
    public final Field field;

    public static final ImmutableMap<Method, BuiltInMethod> MAP;


    static {
        final ImmutableMap.Builder<Method, BuiltInMethod> builder = ImmutableMap.builder();
        for ( BuiltInMethod value : BuiltInMethod.values() ) {
            if ( value.method != null ) {
                builder.put( value.method, value );
            }
        }
        MAP = builder.build();
    }


    BuiltInMethod( Method method, Constructor constructor, Field field ) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }


    /**
     * Defines a method.
     */
    BuiltInMethod( Class clazz, String methodName, Class... argumentTypes ) {
        this( Types.lookupMethod( clazz, methodName, argumentTypes ), null, null );
    }


    /**
     * Defines a constructor.
     */
    BuiltInMethod( Class clazz, Class... argumentTypes ) {
        this( null, Types.lookupConstructor( clazz, argumentTypes ), null );
    }


    /**
     * Defines a field.
     */
    BuiltInMethod( Class clazz, String fieldName, boolean dummy ) {
        this( null, null, Types.lookupField( clazz, fieldName ) );
        assert dummy : "dummy value for method overloading must be true";
    }
}

