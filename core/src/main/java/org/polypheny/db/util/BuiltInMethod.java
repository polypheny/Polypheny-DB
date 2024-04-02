/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.ExtendedEnumerable;
import org.apache.calcite.linq4j.JoinType;
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
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.enumerable.AggregateLambdaFactory;
import org.polypheny.db.algebra.enumerable.BatchIteratorEnumerable;
import org.polypheny.db.algebra.enumerable.OrderedAggregateLambdaFactory;
import org.polypheny.db.algebra.enumerable.SequencedAdderAggregateLambdaFactory;
import org.polypheny.db.algebra.enumerable.SourceSorter;
import org.polypheny.db.algebra.enumerable.lpg.EnumerableLpgMatch.MatchEnumerable;
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
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Selectivity;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.Size;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.TableReferences;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.TupleCount;
import org.polypheny.db.algebra.metadata.BuiltInMetadata.UniqueKeys;
import org.polypheny.db.algebra.metadata.Metadata;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.functions.CrossModelFunctions;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.functions.Functions.FlatProductInputType;
import org.polypheny.db.functions.MqlFunctions;
import org.polypheny.db.functions.TemporalFunctions;
import org.polypheny.db.interpreter.Context;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.interpreter.Scalar;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.ArrayBindable;
import org.polypheny.db.runtime.BinarySearch;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.ComparableList;
import org.polypheny.db.runtime.Enumerables;
import org.polypheny.db.runtime.RandomFunction;
import org.polypheny.db.runtime.SortedMultiMap;
import org.polypheny.db.runtime.Utilities;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;


/**
 * Built-in methods.
 */
public enum BuiltInMethod {
    SWITCH_CONTEXT( DataContext.class, "switchContext" ),
    BATCH( Functions.class, "batch", DataContext.class, Enumerable.class ),
    STREAM_RIGHT( Functions.class, "streamRight", DataContext.class, Enumerable.class, Function0.class, List.class ),
    ENFORCE_CONSTRAINT( Functions.class, "enforceConstraint", DataContext.class, Function0.class, Function0.class, List.class, List.class ),
    PARSE_ARRAY_FROM_TEXT( Functions.class, "reparse", String.class ),
    QUERYABLE_SELECT( Queryable.class, "select", FunctionExpression.class ),
    QUERYABLE_AS_ENUMERABLE( Queryable.class, "asEnumerable" ),
    QUERYABLE_TABLE_AS_QUERYABLE( QueryableEntity.class, "asQueryable", DataContext.class, Snapshot.class ),
    AS_QUERYABLE( Enumerable.class, "asQueryable" ),
    ABSTRACT_ENUMERABLE_CTOR( AbstractEnumerable.class ),
    BATCH_ITERATOR_CTOR( BatchIteratorEnumerable.class ),
    BATCH_ITERATOR_GET_ENUM( BatchIteratorEnumerable.class, "getEnumerable" ),
    INTO( ExtendedEnumerable.class, "into", Collection.class ),
    REMOVE_ALL( ExtendedEnumerable.class, "removeAll", Collection.class ),

    SCHEMA_PLUS_UNWRAP( SchemaPlus.class, "unwrapOrThrow", Class.class ),
    DATA_CONTEXT_GET( DataContext.class, "get", String.class ),
    DATA_CONTEXT_GET_PARAMETER_VALUE( DataContext.class, "getParameterValue", long.class ),
    DATA_CONTEXT_GET_ROOT_SCHEMA( DataContext.class, "getSnapshot" ),

    ROW_VALUE( Row.class, "getObject", int.class ),
    ROW_AS_COPY( Row.class, "asCopy", Object[].class ),
    JOIN( ExtendedEnumerable.class, "hashJoin", Enumerable.class, Function1.class, Function1.class, Function2.class, EqualityComparer.class, boolean.class,
            boolean.class, Predicate2.class ),
    MERGE_JOIN( EnumerableDefaults.class, "mergeJoin", Enumerable.class, Enumerable.class, Function1.class, Function1.class, Function2.class, boolean.class, boolean.class ),
    SLICE0( Enumerables.class, "slice0", Enumerable.class ),
    SEMI_JOIN( EnumerableDefaults.class, "semiJoin", Enumerable.class, Enumerable.class, Function1.class, Function1.class ),
    SINGLE_SUM( Functions.class, "singleSum", Enumerable.class ),
    CORRELATE_JOIN( ExtendedEnumerable.class, "correlateJoin", JoinType.class, Function1.class, Function2.class ),
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
    SINGLETON_ARRAY_ENUMERABLE( Functions.class, "singletonEnumerable", PolyValue.class ),
    EMPTY_ENUMERABLE( Linq4j.class, "emptyEnumerable" ),
    NULLS_COMPARATOR( org.apache.calcite.linq4j.function.Functions.class, "nullsComparator", boolean.class, boolean.class ),
    ARRAY_COMPARER( org.apache.calcite.linq4j.function.Functions.class, "arrayComparer" ),
    FUNCTION0_APPLY( Function0.class, "apply" ),
    FUNCTION1_APPLY( Function1.class, "apply", Object.class ),
    ARRAYS_AS_LIST( Arrays.class, "asList", Object[].class ),
    MAP_OF_ENTRIES( ImmutableMap.class, "copyOf", List.class ),
    ARRAY( Functions.class, "array", Object[].class ),
    FLAT_PRODUCT( Functions.class, "flatProduct", int[].class, boolean.class, FlatProductInputType[].class ),
    LIST_N( PolyList.class, "ofArray", PolyValue[].class ),
    COMPARABLE_EMPTY_LIST( ComparableList.class, "COMPARABLE_EMPTY_LIST", true ),
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
    ARRAY_ITEM( Functions.class, "arrayItemOptional", List.class, PolyNumber.class ),
    MAP_ITEM( Functions.class, "mapItemOptional", Map.class, PolyValue.class ),
    ANY_ITEM( Functions.class, "itemOptional", Map.class, PolyValue.class ),
    UPPER( Functions.class, "upper", PolyString.class ),
    LOWER( Functions.class, "lower", PolyString.class ),
    JSONIZE( Functions.class, "toJson", PolyValue.class ),
    JSON_VALUE_EXPRESSION( Functions.class, "jsonValueExpression", PolyString.class ),
    JSON_VALUE_EXPRESSION_EXCLUDE( Functions.class, "jsonValueExpressionExclude", PolyString.class, List.class ),
    JSON_STRUCTURED_VALUE_EXPRESSION( Functions.class, "jsonStructuredValueExpression", Object.class ),
    JSON_API_COMMON_SYNTAX( Functions.class, "jsonApiCommonSyntax", PolyValue.class, PolyString.class ),
    JSON_EXISTS( Functions.class, "jsonExists", PolyValue.class ),
    JSON_VALUE_ANY( Functions.class, "jsonValueAny", PolyValue.class, JsonValueEmptyOrErrorBehavior.class, PolyValue.class, JsonValueEmptyOrErrorBehavior.class, PolyValue.class ),
    JSON_QUERY( Functions.class, "jsonQuery", PolyValue.class, JsonQueryWrapperBehavior.class, JsonQueryEmptyOrErrorBehavior.class, JsonQueryEmptyOrErrorBehavior.class ),
    JSON_OBJECT( Functions.class, "jsonObject", JsonConstructorNullClause.class ),
    JSON_OBJECTAGG_ADD( Functions.class, "jsonObjectAggAdd", Map.class, String.class, PolyValue.class, JsonConstructorNullClause.class ),
    JSON_ARRAY( Functions.class, "jsonArray", JsonConstructorNullClause.class ),
    JSON_ARRAYAGG_ADD( Functions.class, "jsonArrayAggAdd", List.class, PolyValue.class, JsonConstructorNullClause.class ),
    IS_JSON_VALUE( Functions.class, "isJsonValue", PolyString.class ),
    IS_JSON_OBJECT( Functions.class, "isJsonObject", PolyString.class ),
    IS_JSON_ARRAY( Functions.class, "isJsonArray", PolyString.class ),
    IS_JSON_SCALAR( Functions.class, "isJsonScalar", PolyString.class ),
    INITCAP( Functions.class, "initcap", String.class ),
    SUBSTRING( Functions.class, "substring", PolyString.class, PolyNumber.class, PolyNumber.class ),
    CHAR_LENGTH( Functions.class, "charLength", PolyString.class ),
    STRING_CONCAT( Functions.class, "concat", PolyString.class, PolyString.class ),
    FLOOR_DIV( TemporalFunctions.class, "floorDiv", PolyNumber.class, PolyNumber.class ),
    FLOOR_MOD( TemporalFunctions.class, "floorMod", PolyNumber.class, PolyNumber.class ),
    ADD_MONTHS( TemporalFunctions.class, "addMonths", PolyTimestamp.class, PolyNumber.class ),
    ADD_MONTHS_INT( TemporalFunctions.class, "addMonths", PolyDate.class, PolyNumber.class ),
    SUBTRACT_MONTHS( TemporalFunctions.class, "subtractMonths", PolyDate.class, PolyDate.class ),
    FLOOR( Functions.class, "floor", PolyNumber.class, PolyNumber.class ),
    CEIL( Functions.class, "ceil", PolyNumber.class, PolyNumber.class ),
    OVERLAY( Functions.class, "overlay", PolyString.class, PolyString.class, PolyNumber.class ),
    OVERLAY3( Functions.class, "overlay", PolyString.class, PolyString.class, PolyNumber.class, PolyNumber.class ),
    POSITION( Functions.class, "position", PolyString.class, PolyString.class ),
    RAND( RandomFunction.class, "rand" ),
    RAND_SEED( RandomFunction.class, "randSeed", PolyNumber.class ),
    RAND_INTEGER( RandomFunction.class, "randInteger", PolyNumber.class ),
    RAND_INTEGER_SEED( RandomFunction.class, "randIntegerSeed", PolyNumber.class, PolyNumber.class ),
    TRUNCATE( Functions.class, "truncate", PolyString.class, int.class ),
    TRUNCATE_OR_PAD( Functions.class, "truncateOrPad", PolyString.class, int.class ),
    TRIM( Functions.class, "trim", boolean.class, boolean.class, String.class, String.class, boolean.class ),
    REPLACE( Functions.class, "replace", String.class, String.class, String.class ),
    TRANSLATE3( Functions.class, "translate3", String.class, String.class, String.class ),
    LTRIM( Functions.class, "ltrim", String.class ),
    RTRIM( Functions.class, "rtrim", String.class ),
    LIKE( Functions.class, "like", PolyString.class, PolyString.class ),
    SIMILAR( Functions.class, "similar", PolyString.class, PolyString.class ),
    IS_TRUE( Functions.class, "isTrue", PolyBoolean.class ),
    IS_NOT_FALSE( Functions.class, "isNotFalse", PolyBoolean.class ),
    NOT( Functions.class, "not", PolyBoolean.class ),
    LESSER( Functions.class, "lesser", Comparable.class, Comparable.class ),
    GREATER( Functions.class, "greater", Comparable.class, Comparable.class ),
    BIT_AND( Functions.class, "bitAnd", long.class, long.class ),
    BIT_OR( Functions.class, "bitOr", long.class, long.class ),
    SCANNABLE_TABLE_SCAN( ScannableEntity.class, "scan", DataContext.class ),
    STRING_TO_BOOLEAN( Functions.class, "toBoolean", PolyString.class ),
    INTERNAL_TO_DATE( TemporalFunctions.class, "internalToDate", PolyNumber.class ),
    INTERNAL_TO_TIME( TemporalFunctions.class, "internalToTime", PolyNumber.class ),
    INTERNAL_TO_TIMESTAMP( TemporalFunctions.class, "internalToTimestamp", PolyNumber.class ),
    STRING_TO_DATE( TemporalFunctions.class, "dateStringToUnixDate", PolyString.class ),
    STRING_TO_TIME( TemporalFunctions.class, "timeStringToUnixDate", PolyString.class ),
    STRING_TO_TIMESTAMP( TemporalFunctions.class, "timestampStringToUnixDate", PolyString.class ),
    STRING_TO_TIME_WITH_LOCAL_TIME_ZONE( TemporalFunctions.class, "toTimeWithLocalTimeZone", PolyString.class ),
    TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE( TemporalFunctions.class, "toTimeWithLocalTimeZone", PolyString.class, TimeZone.class ),
    STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE( TemporalFunctions.class, "toTimestampWithLocalTimeZone", PolyString.class ),
    TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE( TemporalFunctions.class, "toTimestampWithLocalTimeZone", PolyString.class, TimeZone.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIME( TemporalFunctions.class, "timeWithLocalTimeZoneToTime", PolyNumber.class, TimeZone.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP( TemporalFunctions.class, "timeWithLocalTimeZoneToTimestamp", PolyString.class, PolyNumber.class, TimeZone.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE( TemporalFunctions.class, "timeWithLocalTimeZoneToTimestampWithLocalTimeZone", PolyString.class, PolyNumber.class ),
    TIME_WITH_LOCAL_TIME_ZONE_TO_STRING( TemporalFunctions.class, "timeWithLocalTimeZoneToString", PolyNumber.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE( TemporalFunctions.class, "timestampWithLocalTimeZoneToDate", PolyNumber.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME( TemporalFunctions.class, "timestampWithLocalTimeZoneToTime", PolyNumber.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE( TemporalFunctions.class, "timestampWithLocalTimeZoneToTimeWithLocalTimeZone", PolyNumber.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP( TemporalFunctions.class, "timestampWithLocalTimeZoneToTimestamp", PolyNumber.class, TimeZone.class ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING( TemporalFunctions.class, "timestampWithLocalTimeZoneToString", PolyNumber.class, TimeZone.class ),
    UNIX_DATE_TO_STRING( TemporalFunctions.class, "unixDateToString", PolyDate.class ),
    UNIX_TIME_TO_STRING( TemporalFunctions.class, "unixTimeToString", PolyTime.class ),
    UNIX_TIMESTAMP_TO_STRING( TemporalFunctions.class, "unixTimestampToString", PolyTimestamp.class ),
    INTERVAL_YEAR_MONTH_TO_STRING( TemporalFunctions.class, "intervalYearMonthToString", PolyInterval.class, TimeUnitRange.class ),
    INTERVAL_DAY_TIME_TO_STRING( TemporalFunctions.class, "intervalDayTimeToString", PolyInterval.class, TimeUnitRange.class, PolyNumber.class ),
    UNIX_DATE_EXTRACT( TemporalFunctions.class, "unixDateExtract", TimeUnitRange.class, PolyTemporal.class ),
    UNIX_DATE_FLOOR( TemporalFunctions.class, "unixDateFloor", TimeUnitRange.class, PolyDate.class ),
    UNIX_DATE_CEIL( TemporalFunctions.class, "unixDateCeil", TimeUnitRange.class, PolyDate.class ),
    UNIX_TIMESTAMP_FLOOR( TemporalFunctions.class, "unixTimestampFloor", TimeUnitRange.class, PolyTimestamp.class ),
    UNIX_TIMESTAMP_CEIL( TemporalFunctions.class, "unixTimestampCeil", TimeUnitRange.class, PolyTimestamp.class ),
    CURRENT_TIMESTAMP( TemporalFunctions.class, "currentTimestamp", DataContext.class ),
    CURRENT_TIME( TemporalFunctions.class, "currentTime", DataContext.class ),
    CURRENT_DATE( TemporalFunctions.class, "currentDate", DataContext.class ),
    LOCAL_TIMESTAMP( TemporalFunctions.class, "localTimestamp", DataContext.class ),
    LOCAL_TIME( TemporalFunctions.class, "localTime", DataContext.class ),
    TIME_ZONE( TemporalFunctions.class, "timeZone", DataContext.class ),
    BOOLEAN_TO_STRING( Functions.class, "toString", PolyBoolean.class ),

    MILLIS_SINCE_EPOCH( PolyTemporal.class, "getMillisSinceEpoch" ),

    MILLIS_SINCE_EPOCH_POLY( PolyTemporal.class, "getPolyMillisSinceEpoch" ),

    JDBC_DEEP_ARRAY_TO_POLY_LIST( Functions.class, "arrayToPolyList", java.sql.Array.class, Function1.class, int.class ),
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
    DATE_TO_LONG( TemporalFunctions.class, "dateToLong", java.util.Date.class ),
    DATE_TO_LONG_OPTIONAL( TemporalFunctions.class, "dateToLongOptional", java.util.Date.class ),
    TIME_TO_LONG( TemporalFunctions.class, "timeToLong", Time.class ),
    TIME_TO_LONG_OPTIONAL( TemporalFunctions.class, "timeToLongOptional", Time.class ),
    TIMESTAMP_TO_LONG_OFFSET( TemporalFunctions.class, "toLong", java.util.Date.class, TimeZone.class ),
    TIMESTAMP_TO_LONG_OPTIONAL_OFFSET( TemporalFunctions.class, "toLongOptional", Timestamp.class ),
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
    TUPLE_COUNT( TupleCount.class, "getTupleCount" ),
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
    SCALAR_EXECUTE2( Scalar.class, "execute", Context.class, PolyValue[].class ),
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
    MQL_EQ( MqlFunctions.class, "docEq", PolyValue.class, PolyValue.class ),
    MQL_GT( MqlFunctions.class, "docGt", PolyValue.class, PolyValue.class ),
    MQL_GTE( MqlFunctions.class, "docGte", PolyValue.class, PolyValue.class ),
    MQL_LT( MqlFunctions.class, "docLt", PolyValue.class, PolyValue.class ),
    MQL_LTE( MqlFunctions.class, "docLte", PolyValue.class, PolyValue.class ),
    MQL_SIZE_MATCH( MqlFunctions.class, "docSizeMatch", PolyValue.class, PolyValue.class ),
    MQL_JSON_MATCH( MqlFunctions.class, "docJsonMatch", PolyValue.class, String.class ),
    MQL_REGEX_MATCH( MqlFunctions.class, "docRegexMatch", PolyValue.class, PolyString.class, PolyBoolean.class, PolyBoolean.class, PolyBoolean.class, PolyBoolean.class ),
    MQL_TYPE_MATCH( MqlFunctions.class, "docTypeMatch", PolyValue.class, List.class ),
    MQL_SLICE( MqlFunctions.class, "docSlice", PolyValue.class, PolyNumber.class, PolyNumber.class ),
    MQL_QUERY_VALUE( MqlFunctions.class, "docQueryValue", PolyValue.class, List.class ),
    MQL_ADD_FIELDS( MqlFunctions.class, "docAddFields", PolyValue.class, List.class, PolyValue.class ),
    MQL_UPDATE_MIN( MqlFunctions.class, "docUpdateMin", PolyValue.class, PolyValue.class ),
    MQL_UPDATE_MAX( MqlFunctions.class, "docUpdateMax", PolyValue.class, PolyValue.class ),
    MQL_UPDATE_ADD_TO_SET( MqlFunctions.class, "docAddToSet", PolyValue.class, PolyValue.class ),
    MQL_REMOVE( MqlFunctions.class, "docRemove", PolyValue.class, List.class ),
    MQL_UPDATE_REPLACE( MqlFunctions.class, "docUpdateReplace", PolyValue.class, List.class, List.class ),
    MQL_UPDATE_RENAME( MqlFunctions.class, "docUpdateRename", PolyValue.class, List.class, List.class ),
    MQL_GET_ARRAY( MqlFunctions.class, "docGetArray", PolyValue.class ),
    MQL_EXISTS( MqlFunctions.class, "docExists", PolyValue.class, PolyValue.class, List.class ),
    MQL_MERGE( MqlFunctions.class, "mergeDocument", PolyValue.class, PolyList.class, PolyValue[].class ),
    MQL_NOT_UNSET( MqlFunctions.class, "notUnset", PolyValue.class ),

    MQL_PROJECT_INCLUDES( MqlFunctions.class, "projectIncludes", PolyValue.class, PolyList.class, PolyValue[].class ),
    MQL_REPLACE_ROOT( MqlFunctions.class, "replaceRoot", PolyValue.class ),
    CYPHER_LIKE( CypherFunctions.class, "like", PolyValue.class, PolyValue.class ),
    CYPHER_PATH_MATCH( CypherFunctions.class, "pathMatch", PolyGraph.class, PolyPath.class ),
    CYPHER_HAS_LABEL( CypherFunctions.class, "hasLabel", PolyNode.class, PolyString.class ),
    CYPHER_HAS_PROPERTY( CypherFunctions.class, "hasProperty", PolyNode.class, PolyString.class ),
    CYPHER_NODE_MATCH( CypherFunctions.class, "nodeMatch", PolyGraph.class, PolyNode.class ),
    CYPHER_NODE_EXTRACT( CypherFunctions.class, "nodeExtract", PolyGraph.class ),
    CYPHER_MATCH_CTOR( MatchEnumerable.class, List.class ),
    CYPHER_EXTRACT_FROM_PATH( CypherFunctions.class, "extractFrom", PolyPath.class, PolyString.class ),
    CYPHER_EXTRACT_PROPERTY( CypherFunctions.class, "extractProperty", GraphPropertyHolder.class, PolyString.class ),
    CYPHER_EXTRACT_PROPERTIES( CypherFunctions.class, "extractProperties", GraphPropertyHolder.class ),
    CYPHER_EXTRACT_ID( CypherFunctions.class, "extractId", GraphPropertyHolder.class ),
    CYPHER_EXTRACT_LABELS( CypherFunctions.class, "extractLabels", GraphPropertyHolder.class ),
    CYPHER_EXTRACT_LABEL( CypherFunctions.class, "extractLabel", GraphPropertyHolder.class ),
    CYPHER_TO_LIST( CypherFunctions.class, "toList", PolyValue.class ),
    CYPHER_ADJUST_EDGE( CypherFunctions.class, "adjustEdge", PolyEdge.class, PolyNode.class, PolyNode.class ),
    CYPHER_SET_PROPERTY( CypherFunctions.class, "setProperty", GraphPropertyHolder.class, PolyString.class, PolyValue.class ),
    CYPHER_SET_PROPERTIES( CypherFunctions.class, "setProperties", GraphPropertyHolder.class, List.class, List.class, PolyBoolean.class ),
    CYPHER_SET_LABELS( CypherFunctions.class, "setLabels", GraphPropertyHolder.class, List.class, PolyBoolean.class ),
    CYPHER_REMOVE_LABELS( CypherFunctions.class, "removeLabels", GraphPropertyHolder.class, List.class ),
    CYPHER_REMOVE_PROPERTY( CypherFunctions.class, "removeProperty", GraphPropertyHolder.class, String.class ),
    TO_NODE( CypherFunctions.class, "toNode", Enumerable.class ),
    TO_EDGE( CypherFunctions.class, "toEdge", Enumerable.class ),
    TO_GRAPH( CypherFunctions.class, "toGraph", Enumerable.class, Enumerable.class ),
    SPLIT_GRAPH_MODIFY( CrossModelFunctions.class, "sendGraphModifies", DataContext.class, List.class, List.class, Operation.class ),
    X_MODEL_TABLE_TO_NODE( CrossModelFunctions.class, "tableToNodes", Enumerable.class, PolyString.class, List.class ),
    X_MODEL_MERGE_NODE_COLLECTIONS( CrossModelFunctions.class, "mergeNodeCollections", List.class ),
    X_MODEL_COLLECTION_TO_NODE( CrossModelFunctions.class, "collectionToNodes", Enumerable.class, PolyString.class ),
    X_MODEL_NODE_TO_COLLECTION( CrossModelFunctions.class, "nodesToCollection", Enumerable.class, PolyString.class ),

    X_MODEL_ITEM( CrossModelFunctions.class, "docItem", String.class, String.class ),
    SINGLE_TO_ARRAY_ENUMERABLE( Functions.class, "singleToArray", Enumerable.class ),
    X_MODEL_GRAPH_ONLY_LABEL( CrossModelFunctions.class, "cypherOnlyLabelGraph", PolyValue.class, PolyString.class ),
    TO_JSON( PolyValue.class, "toPolyJson" );

    private static final String toIntOptional = "toIntOptional";
    public final Method method;
    public final Constructor<?> constructor;
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


    BuiltInMethod( Method method, Constructor<?> constructor, Field field ) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }


    /**
     * Defines a method.
     */
    BuiltInMethod( Class<?> clazz, String methodName, Class<?>... argumentTypes ) {
        this( Types.lookupMethod( clazz, methodName, argumentTypes ), null, null );
    }


    /**
     * Defines a constructor.
     */
    BuiltInMethod( Class<?> clazz, Class<?>... argumentTypes ) {
        this( null, Types.lookupConstructor( clazz, argumentTypes ), null );
    }


    /**
     * Defines a field.
     */
    BuiltInMethod( Class<?> clazz, String fieldName, boolean dummy ) {
        this( null, null, Types.lookupField( clazz, fieldName ) );
        assert dummy : "dummy value for method overloading must be true";
    }
}

