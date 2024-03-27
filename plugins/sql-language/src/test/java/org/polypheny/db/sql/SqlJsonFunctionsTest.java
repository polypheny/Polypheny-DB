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
 */

package org.polypheny.db.sql;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.jayway.jsonpath.PathNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.bson.json.JsonParseException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.json.JsonConstructorNullClause;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryEmptyOrErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryWrapperBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.functions.PathContext;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.relational.PolyMap;


/**
 * Unit test for the methods in {@link Functions} that implement JSON processing functions.
 */
public class SqlJsonFunctionsTest extends SqlLanguageDependent {

    public static final String INVOC_DESC_JSON_VALUE_EXPRESSION = "jsonValueExpression";
    public static final String INVOC_DESC_JSON_STRUCTURED_VALUE_EXPRESSION = "jsonStructuredValueExpression";
    public static final String INVOC_DESC_JSON_API_COMMON_SYNTAX = "jsonApiCommonSyntax";
    public static final String INVOC_DESC_JSON_EXISTS = "jsonExists";
    public static final String INVOC_DESC_JSON_VALUE_ANY = "jsonValueAny";
    public static final String INVOC_DESC_JSON_QUERY = "jsonQuery";
    public static final String INVOC_DESC_JSONIZE = "toJson";
    public static final String INVOC_DESC_DEJSONIZE = "dejsonize";
    public static final String INVOC_DESC_JSON_OBJECT = "jsonObject";
    public static final String INVOC_DESC_JSON_OBJECT_AGG_ADD = "jsonObjectAggAdd";
    public static final String INVOC_DESC_JSON_ARRAY = "jsonArray";
    public static final String INVOC_DESC_JSON_ARRAY_AGG_ADD = "jsonArrayAggAdd";
    public static final String INVOC_DESC_IS_JSON_VALUE = "isJsonValue";
    public static final String INVOC_DESC_IS_JSON_SCALAR = "isJsonScalar";
    public static final String INVOC_DESC_IS_JSON_ARRAY = "isJsonArray";
    public static final String INVOC_DESC_IS_JSON_OBJECT = "isJsonObject";


    @Test
    public void testJsonValueExpression() {
        assertJsonValueExpression( "{}", is( PolyDocument.EMPTY_DOCUMENT ) );
    }


    @Test
    public void testJsonStructuredValueExpression() {
        assertJsonStructuredValueExpression( "bar", is( "bar" ) );
        assertJsonStructuredValueExpression( 100, is( 100 ) );
    }


    @Test
    public void testJsonApiCommonSyntax() {
        assertJsonApiCommonSyntax(
                PolyDocument.of( Map.of( PolyString.of( "foo" ), PolyString.of( "bar" ) ) ),
                PolyString.of( "lax $.foo" ),
                contextMatches( PathContext.withReturned( Functions.PathMode.LAX, PolyString.of( "bar" ) ) ) );
        assertJsonApiCommonSyntax(
                PolyDocument.of( Map.of( PolyString.of( "foo" ), PolyString.of( "bar" ) ) ),
                PolyString.of( "strict $.foo" ),
                contextMatches( PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ) ) );
        assertJsonApiCommonSyntax(
                PolyDocument.of( Map.of( PolyString.of( "foo" ), PolyString.of( "bar" ) ) ),
                PolyString.of( "lax $.foo1" ),
                contextMatches( PathContext.withReturned( Functions.PathMode.LAX, null ) ) );
        assertJsonApiCommonSyntax(
                PolyDocument.of( Map.of( PolyString.of( "foo" ), PolyString.of( "bar" ) ) ),
                PolyString.of( "strict $.foo1" ),
                contextMatches( PathContext.withStrictException( new PathNotFoundException( "No results for path: $['foo1']" ) ) ) );
        assertJsonApiCommonSyntax(
                PolyDocument.of( Map.of( PolyString.of( "foo" ), PolyInteger.of( 100 ) ) ),
                PolyString.of( "lax $.foo" ),
                contextMatches( PathContext.withReturned( Functions.PathMode.LAX, PolyInteger.of( 100 ) ) ) );
    }


    @Test
    public void testJsonExists() {
        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonExistsErrorBehavior.FALSE,
                is( true ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonExistsErrorBehavior.TRUE,
                is( true ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonExistsErrorBehavior.UNKNOWN,
                is( true ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonExistsErrorBehavior.ERROR,
                is( true ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.FALSE,
                is( false ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.TRUE,
                is( false ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.UNKNOWN,
                is( false ) );

        assertJsonExists(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.ERROR,
                is( false ) );

        assertJsonExists(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.FALSE,
                is( false ) );

        assertJsonExists(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.TRUE,
                is( true ) );

        assertJsonExists(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.UNKNOWN,
                nullValue() );

        assertJsonExistsFailed(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.ERROR,
                errorMatches( new GenericRuntimeException( "java.lang.Exception: test message" ) ) );
    }


    @Test
    public void testJsonValueAny() {
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.LAX, PolyString.of( "bar" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                is( PolyString.of( "bar" ) ) );
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                PolyString.of( "empty" ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                is( PolyString.of( "empty" ) ) );
        assertJsonValueAnyFailed(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_VALUE function is not allowed", null ) ) );
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.LAX, PolyList.EMPTY_LIST ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.LAX, PolyList.EMPTY_LIST ),
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                PolyString.of( "empty" ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                is( PolyString.of( "empty" ) ) );
        assertJsonValueAnyFailed(
                PathContext.withReturned( Functions.PathMode.LAX, PolyList.EMPTY_LIST ),
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_VALUE function is not allowed", null ) ) );
        assertJsonValueAny(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                PolyString.of( "empty" ),
                is( PolyString.of( "empty" ) ) );
        assertJsonValueAnyFailed(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                errorMatches( new GenericRuntimeException( "java.lang.Exception: test message" ) ) );
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyList.EMPTY_LIST ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyList.EMPTY_LIST ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                PolyString.of( "empty" ),
                is( PolyString.of( "empty" ) ) );
        assertJsonValueAnyFailed(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyList.EMPTY_LIST ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                errorMatches(
                        new PolyphenyDbException( "Strict jsonpath mode requires scalar value, and the actual value is: '[]'", null ) ) );
    }


    @Test
    public void testJsonQuery() {
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, PolyList.of( PolyString.of( "bar" ) ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                nullValue() );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[]" ) );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "{}" ) );
        assertJsonQueryFailed(
                PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                JsonQueryEmptyOrErrorBehavior.NULL,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_QUERY function is not allowed", null ) ) );

        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                nullValue() );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[]" ) );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.LAX, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "{}" ) );
        assertJsonQueryFailed(
                PathContext.withReturned( Functions.PathMode.LAX, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                JsonQueryEmptyOrErrorBehavior.NULL,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_QUERY function is not allowed", null ) ) );
        assertJsonQuery(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                is( "[]" ) );
        assertJsonQuery(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                is( "{}" ) );
        assertJsonQueryFailed(
                PathContext.withStrictException( new Exception( "test message" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                errorMatches( new GenericRuntimeException( "java.lang.Exception: test message" ) ) );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                nullValue() );
        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                is( "[]" ) );
        assertJsonQueryFailed(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                errorMatches( new PolyphenyDbException( "Strict jsonpath mode requires array or object value, and the actual value is: 'bar'", null ) ) );

        // wrapper behavior test

        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );

        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyString.of( "bar" ) ),
                JsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );

        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyList.of( PolyString.of( "bar" ) ) ),
                JsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[[\"bar\"]]" ) );

        assertJsonQuery(
                PathContext.withReturned( Functions.PathMode.STRICT, PolyList.of( PolyString.of( "bar" ) ) ),
                JsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );
    }


    @Test
    public void testMapJsonize() {
        assertJsonize( PolyMap.EMPTY_MAP, is( "{}" ) );
    }


    @Test
    public void testDocJsonize() {
        assertJsonize( PolyDocument.EMPTY_DOCUMENT, is( "{}" ) );
    }


    @Test
    public void testDejsonize() {
        assertDejsonize( "{}", is( PolyDocument.EMPTY_DOCUMENT ) );
        assertDejsonize( "[]", is( PolyList.EMPTY_LIST ) );

        // expect exception thrown
        final String message = "JSON reader was expecting a value but found '}'.";
        assertDejsonizeFailed( "[}", errorMatches( new JsonParseException( message ) ) );
    }


    @Test
    public void testJsonObject() {
        assertJsonObject( is( "{}" ), JsonConstructorNullClause.NULL_ON_NULL );
        assertJsonObject(
                is( "{\"foo\":\"bar\"}" ),
                JsonConstructorNullClause.NULL_ON_NULL,
                PolyString.of( "foo" ),
                PolyString.of( "bar" ) );
        assertJsonObject(
                is( "{\"foo\":null}" ),
                JsonConstructorNullClause.NULL_ON_NULL,
                PolyString.of( "foo" ),
                null );
        assertJsonObject(
                is( "{}" ),
                JsonConstructorNullClause.ABSENT_ON_NULL,
                PolyString.of( "foo" ),
                null );
    }


    @Test
    public void testJsonObjectAggAdd() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> expected = new HashMap<>();
        expected.put( "foo", "bar" );
        assertJsonObjectAggAdd( map, "foo", "bar", JsonConstructorNullClause.NULL_ON_NULL, is( expected ) );
        expected.put( "foo1", null );
        assertJsonObjectAggAdd( map, "foo1", null, JsonConstructorNullClause.NULL_ON_NULL, is( expected ) );
        assertJsonObjectAggAdd( map, "foo2", null, JsonConstructorNullClause.ABSENT_ON_NULL, is( expected ) );
    }


    @Test
    public void testJsonArray() {
        assertJsonArray( is( "[]" ), JsonConstructorNullClause.NULL_ON_NULL );
        assertJsonArray( is( "[\"foo\"]" ), JsonConstructorNullClause.NULL_ON_NULL, PolyString.of( "foo" ) );
        assertJsonArray( is( "[\"foo\",null]" ), JsonConstructorNullClause.NULL_ON_NULL, PolyString.of( "foo" ), null );
        assertJsonArray( is( "[\"foo\"]" ), JsonConstructorNullClause.ABSENT_ON_NULL, PolyString.of( "foo" ), null );
    }


    @Test
    public void testJsonArrayAggAdd() {
        List<PolyValue> list = new ArrayList<>();
        List<PolyValue> expected = new ArrayList<>();
        expected.add( PolyString.of( "foo" ) );
        assertJsonArrayAggAdd( list, PolyString.of( "foo" ), JsonConstructorNullClause.NULL_ON_NULL, is( expected ) );
        expected.add( null );
        assertJsonArrayAggAdd( list, null, JsonConstructorNullClause.NULL_ON_NULL, is( expected ) );
        assertJsonArrayAggAdd( list, null, JsonConstructorNullClause.ABSENT_ON_NULL, is( expected ) );
    }


    @Test
    public void testJsonPredicate() {
        assertIsJsonValue( "[]", is( true ) );
        assertIsJsonValue( "{}", is( true ) );
        assertIsJsonValue( "100", is( true ) );
        assertIsJsonValue( "{]", is( false ) );
        assertIsJsonObject( "[]", is( false ) );
        assertIsJsonObject( "{}", is( true ) );
        assertIsJsonObject( "100", is( false ) );
        assertIsJsonObject( "{]", is( false ) );
        assertIsJsonArray( "[]", is( true ) );
        assertIsJsonArray( "{}", is( false ) );
        assertIsJsonArray( "100", is( false ) );
        assertIsJsonArray( "{]", is( false ) );
        assertIsJsonScalar( "[]", is( false ) );
        assertIsJsonScalar( "{}", is( false ) );
        assertIsJsonScalar( "100", is( true ) );
        assertIsJsonScalar( "{]", is( false ) );
    }


    private void assertJsonValueExpression( String input, Matcher<Object> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_VALUE_EXPRESSION, input ),
                Functions.jsonValueExpression( PolyString.of( input ) ), matcher );
    }


    private void assertJsonStructuredValueExpression( Object input, Matcher<Object> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_STRUCTURED_VALUE_EXPRESSION, input ),
                Functions.jsonStructuredValueExpression( input ), matcher );
    }


    private void assertJsonApiCommonSyntax( PolyValue input, PolyString pathSpec, Matcher<? super PathContext> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_API_COMMON_SYNTAX, input, pathSpec ),
                Functions.jsonApiCommonSyntax( input, pathSpec ), matcher );
    }


    private void assertJsonExists( PathContext input, JsonExistsErrorBehavior errorBehavior, Matcher<? super Boolean> matcher ) {
        assertThat( invocationDesc( INVOC_DESC_JSON_EXISTS, input, errorBehavior ), Functions.jsonExists( input, errorBehavior ), matcher );
    }


    private void assertJsonExistsFailed( PathContext input, JsonExistsErrorBehavior errorBehavior, Matcher<? super Throwable> matcher ) {
        assertFailed( invocationDesc( INVOC_DESC_JSON_EXISTS, input, errorBehavior ), () -> Functions.jsonExists( input, errorBehavior ), matcher );
    }


    private void assertJsonValueAny( PathContext input, JsonValueEmptyOrErrorBehavior emptyBehavior, PolyValue defaultValueOnEmpty, JsonValueEmptyOrErrorBehavior errorBehavior, PolyValue defaultValueOnError, Matcher<Object> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_VALUE_ANY, input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                Functions.jsonValueAny( input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                matcher );
    }


    private void assertJsonValueAnyFailed( PathContext input, JsonValueEmptyOrErrorBehavior emptyBehavior, PolyValue defaultValueOnEmpty, JsonValueEmptyOrErrorBehavior errorBehavior, PolyValue defaultValueOnError, Matcher<? super Throwable> matcher ) {
        assertFailed(
                invocationDesc( INVOC_DESC_JSON_VALUE_ANY, input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                () -> Functions.jsonValueAny( input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                matcher );
    }


    private void assertJsonQuery( PathContext input, JsonQueryWrapperBehavior wrapperBehavior, JsonQueryEmptyOrErrorBehavior emptyBehavior, JsonQueryEmptyOrErrorBehavior errorBehavior, Matcher<? super String> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_QUERY, input, wrapperBehavior, emptyBehavior, errorBehavior ),
                Functions.jsonQuery( input, wrapperBehavior, emptyBehavior, errorBehavior ),
                matcher );
    }


    private void assertJsonQueryFailed( PathContext input, JsonQueryWrapperBehavior wrapperBehavior, JsonQueryEmptyOrErrorBehavior emptyBehavior, JsonQueryEmptyOrErrorBehavior errorBehavior, Matcher<? super Throwable> matcher ) {
        assertFailed(
                invocationDesc( INVOC_DESC_JSON_QUERY, input, wrapperBehavior, emptyBehavior, errorBehavior ),
                () -> Functions.jsonQuery( input, wrapperBehavior, emptyBehavior, errorBehavior ),
                matcher );
    }


    private void assertJsonize( PolyValue input, Matcher<? super String> matcher ) {
        assertThat( invocationDesc( INVOC_DESC_JSONIZE, input ), Functions.toJson( input ), matcher );
    }


    private void assertDejsonize( String input, Matcher<Object> matcher ) {
        assertThat( invocationDesc( INVOC_DESC_DEJSONIZE, input ), Functions.dejsonize( PolyString.of( input ) ), matcher );
    }


    private void assertDejsonizeFailed( String input, Matcher<? super Throwable> matcher ) {
        assertFailed( invocationDesc( INVOC_DESC_DEJSONIZE, input ), () -> Functions.dejsonize( PolyString.of( input ) ), matcher );
    }


    private void assertJsonObject( Matcher<? super String> matcher, JsonConstructorNullClause nullClause, PolyValue... kvs ) {
        assertThat( invocationDesc( INVOC_DESC_JSON_OBJECT, nullClause, kvs ), Functions.jsonObject( nullClause, kvs ), matcher );
    }


    private void assertJsonObjectAggAdd( Map<String, Object> map, String k, Object v, JsonConstructorNullClause nullClause, Matcher<? super Map<?, ?>> matcher ) {
        Functions.jsonObjectAggAdd( map, k, v, nullClause );
        assertThat(
                invocationDesc( INVOC_DESC_JSON_OBJECT_AGG_ADD, map, k, v, nullClause ), map, matcher );
    }


    private void assertJsonArray( Matcher<? super String> matcher, JsonConstructorNullClause nullClause, PolyValue... elements ) {
        assertThat( invocationDesc( INVOC_DESC_JSON_ARRAY, nullClause, elements ), Functions.jsonArray( nullClause, elements ), matcher );
    }


    private void assertJsonArrayAggAdd( List<PolyValue> list, PolyValue element, JsonConstructorNullClause nullClause, Matcher<? super List<?>> matcher ) {
        Functions.jsonArrayAggAdd( list, element, nullClause );
        assertThat(
                invocationDesc( INVOC_DESC_JSON_ARRAY_AGG_ADD, list, element, nullClause ),
                list, matcher );
    }


    private void assertIsJsonValue( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_VALUE, input ),
                Functions.isJsonValue( PolyString.of( input ) ),
                matcher );
    }


    private void assertIsJsonScalar( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_SCALAR, input ),
                Functions.isJsonScalar( PolyString.of( input ) ),
                matcher );
    }


    private void assertIsJsonArray( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_ARRAY, input ),
                Functions.isJsonArray( PolyString.of( input ) ),
                matcher );
    }


    private void assertIsJsonObject( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_OBJECT, input ),
                Functions.isJsonObject( PolyString.of( input ) ),
                matcher );
    }


    private String invocationDesc( String methodName, Object... args ) {
        return methodName + "(" + Arrays.stream( args )
                .map( Objects::toString )
                .collect( Collectors.joining( ", " ) ) + ")";
    }


    private void assertFailed( String invocationDesc, Supplier<?> supplier, Matcher<? super Throwable> matcher ) {
        try {
            supplier.get();
            fail( "expect exception, but not: " + invocationDesc );
        } catch ( Throwable t ) {
            assertThat( invocationDesc, t, matcher );
        }
    }


    private Matcher<? super Throwable> errorMatches( Throwable expected ) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches( Object item ) {
                if ( !(item instanceof Throwable error) ) {
                    return false;
                }
                return expected != null
                        && Objects.equals( error.getClass(), expected.getClass() )
                        && Objects.equals( error.getMessage(), expected.getMessage() );
            }


            @Override
            public void describeTo( Description description ) {
                description.appendText( "is " ).appendText( expected.toString() );
            }
        };
    }


    @Nonnull
    private BaseMatcher<PathContext> contextMatches( PathContext expected ) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches( Object item ) {
                if ( !(item instanceof PathContext) ) {
                    return false;
                }
                PathContext context = (PathContext) item;
                if ( Objects.equals( context.mode, expected.mode ) && Objects.equals( context.pathReturned, expected.pathReturned ) ) {
                    if ( context.exc == null && expected.exc == null ) {
                        return true;
                    }
                    return context.exc != null
                            && expected.exc != null
                            && Objects.equals( context.exc.getClass(), expected.exc.getClass() )
                            && Objects.equals( context.exc.getMessage(), expected.exc.getMessage() );
                }
                return false;
            }


            @Override
            public void describeTo( Description description ) {
                description.appendText( "is " ).appendText( expected.toString() );
            }
        };
    }

}
