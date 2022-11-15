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
 */

package org.polypheny.db.sql;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.PathNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.polypheny.db.algebra.json.JsonConstructorNullClause;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryEmptyOrErrorBehavior;
import org.polypheny.db.algebra.json.JsonQueryWrapperBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.functions.Functions;


/**
 * Unit test for the methods in {@link Functions} that implement JSON processing functions.
 */
public class SqlJsonFunctionsTest {

    public static final String INVOC_DESC_JSON_VALUE_EXPRESSION = "jsonValueExpression";
    public static final String INVOC_DESC_JSON_STRUCTURED_VALUE_EXPRESSION = "jsonStructuredValueExpression";
    public static final String INVOC_DESC_JSON_API_COMMON_SYNTAX = "jsonApiCommonSyntax";
    public static final String INVOC_DESC_JSON_EXISTS = "jsonExists";
    public static final String INVOC_DESC_JSON_VALUE_ANY = "jsonValueAny";
    public static final String INVOC_DESC_JSON_QUERY = "jsonQuery";
    public static final String INVOC_DESC_JSONIZE = "jsonize";
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
        assertJsonValueExpression( "{}", is( Collections.emptyMap() ) );
    }


    @Test
    public void testJsonStructuredValueExpression() {
        assertJsonStructuredValueExpression( "bar", is( "bar" ) );
        assertJsonStructuredValueExpression( 100, is( 100 ) );
    }


    @Test
    public void testJsonApiCommonSyntax() {
        assertJsonApiCommonSyntax(
                ImmutableMap.of( "foo", "bar" ),
                "lax $.foo",
                contextMatches( Functions.PathContext.withReturned( Functions.PathMode.LAX, "bar" ) ) );
        assertJsonApiCommonSyntax(
                ImmutableMap.of( "foo", "bar" ),
                "strict $.foo",
                contextMatches( Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ) ) );
        assertJsonApiCommonSyntax(
                ImmutableMap.of( "foo", "bar" ),
                "lax $.foo1",
                contextMatches( Functions.PathContext.withReturned( Functions.PathMode.LAX, null ) ) );
        assertJsonApiCommonSyntax(
                ImmutableMap.of( "foo", "bar" ),
                "strict $.foo1",
                contextMatches( Functions.PathContext.withStrictException( new PathNotFoundException( "No results for path: $['foo1']" ) ) ) );
        assertJsonApiCommonSyntax(
                ImmutableMap.of( "foo", 100 ),
                "lax $.foo",
                contextMatches( Functions.PathContext.withReturned( Functions.PathMode.LAX, 100 ) ) );
    }


    @Test
    public void testJsonExists() {
        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonExistsErrorBehavior.FALSE,
                is( true ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonExistsErrorBehavior.TRUE,
                is( true ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonExistsErrorBehavior.UNKNOWN,
                is( true ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonExistsErrorBehavior.ERROR,
                is( true ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.FALSE,
                is( false ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.TRUE,
                is( false ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.UNKNOWN,
                is( false ) );

        assertJsonExists(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonExistsErrorBehavior.ERROR,
                is( false ) );

        assertJsonExists(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.FALSE,
                is( false ) );

        assertJsonExists(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.TRUE,
                is( true ) );

        assertJsonExists(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.UNKNOWN,
                nullValue() );

        assertJsonExistsFailed(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonExistsErrorBehavior.ERROR,
                errorMatches( new RuntimeException( "java.lang.Exception: test message" ) ) );
    }


    @Test
    public void testJsonValueAny() {
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, "bar" ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                is( "bar" ) );
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                "empty",
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                is( "empty" ) );
        assertJsonValueAnyFailed(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_VALUE function is not allowed", null ) ) );
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, Collections.emptyList() ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, Collections.emptyList() ),
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                "empty",
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                is( "empty" ) );
        assertJsonValueAnyFailed(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, Collections.emptyList() ),
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_VALUE function is not allowed", null ) ) );
        assertJsonValueAny(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                "empty",
                is( "empty" ) );
        assertJsonValueAnyFailed(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.ERROR,
                null,
                errorMatches( new RuntimeException( "java.lang.Exception: test message" ) ) );
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, Collections.emptyList() ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                nullValue() );
        assertJsonValueAny(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, Collections.emptyList() ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.DEFAULT,
                "empty",
                is( "empty" ) );
        assertJsonValueAnyFailed(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, Collections.emptyList() ),
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
                Functions.PathContext.withReturned( Functions.PathMode.LAX, Collections.singletonList( "bar" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                nullValue() );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[]" ) );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "{}" ) );
        assertJsonQueryFailed(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, null ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                JsonQueryEmptyOrErrorBehavior.NULL,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_QUERY function is not allowed", null ) ) );

        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                nullValue() );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[]" ) );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "{}" ) );
        assertJsonQueryFailed(
                Functions.PathContext.withReturned( Functions.PathMode.LAX, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                JsonQueryEmptyOrErrorBehavior.NULL,
                errorMatches( new PolyphenyDbException( "Empty result of JSON_QUERY function is not allowed", null ) ) );
        assertJsonQuery(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                is( "[]" ) );
        assertJsonQuery(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT,
                is( "{}" ) );
        assertJsonQueryFailed(
                Functions.PathContext.withStrictException( new Exception( "test message" ) ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                errorMatches( new RuntimeException( "java.lang.Exception: test message" ) ) );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                nullValue() );
        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY,
                is( "[]" ) );
        assertJsonQueryFailed(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonQueryWrapperBehavior.WITHOUT_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.ERROR,
                errorMatches( new PolyphenyDbException( "Strict jsonpath mode requires array or object value, and the actual value is: 'bar'", null ) ) );

        // wrapper behavior test

        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );

        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, "bar" ),
                JsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );

        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, Collections.singletonList( "bar" ) ),
                JsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[[\"bar\"]]" ) );

        assertJsonQuery(
                Functions.PathContext.withReturned( Functions.PathMode.STRICT, Collections.singletonList( "bar" ) ),
                JsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY,
                JsonQueryEmptyOrErrorBehavior.NULL,
                JsonQueryEmptyOrErrorBehavior.NULL,
                is( "[\"bar\"]" ) );
    }


    @Test
    public void testJsonize() {
        assertJsonize( new HashMap<>(), is( "{}" ) );
    }


    @Test
    public void testDejsonize() {
        assertDejsonize( "{}", is( Collections.emptyMap() ) );
        assertDejsonize( "[]", is( Collections.emptyList() ) );

        // expect exception thrown
        final String message = "com.fasterxml.jackson.core.JsonParseException: Unexpected close marker '}': expected ']' (for Array starting at [Source: (String)\"[}\"; line: 1, column: 1])\n at [Source: (String)\"[}\"; line: 1, column: 3]";
        assertDejsonizeFailed( "[}", errorMatches( new InvalidJsonException( message ) ) );
    }


    @Test
    public void testJsonObject() {
        assertJsonObject( is( "{}" ), JsonConstructorNullClause.NULL_ON_NULL );
        assertJsonObject(
                is( "{\"foo\":\"bar\"}" ),
                JsonConstructorNullClause.NULL_ON_NULL,
                "foo",
                "bar" );
        assertJsonObject(
                is( "{\"foo\":null}" ),
                JsonConstructorNullClause.NULL_ON_NULL,
                "foo",
                null );
        assertJsonObject(
                is( "{}" ),
                JsonConstructorNullClause.ABSENT_ON_NULL,
                "foo",
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
        assertJsonArray( is( "[\"foo\"]" ), JsonConstructorNullClause.NULL_ON_NULL, "foo" );
        assertJsonArray( is( "[\"foo\",null]" ), JsonConstructorNullClause.NULL_ON_NULL, "foo", null );
        assertJsonArray( is( "[\"foo\"]" ), JsonConstructorNullClause.ABSENT_ON_NULL, "foo", null );
    }


    @Test
    public void testJsonArrayAggAdd() {
        List<Object> list = new ArrayList<>();
        List<Object> expected = new ArrayList<>();
        expected.add( "foo" );
        assertJsonArrayAggAdd( list, "foo", JsonConstructorNullClause.NULL_ON_NULL, is( expected ) );
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
                Functions.jsonValueExpression( input ), matcher );
    }


    private void assertJsonStructuredValueExpression( Object input, Matcher<Object> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_STRUCTURED_VALUE_EXPRESSION, input ),
                Functions.jsonStructuredValueExpression( input ), matcher );
    }


    private void assertJsonApiCommonSyntax( Object input, String pathSpec, Matcher<? super Functions.PathContext> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_API_COMMON_SYNTAX, input, pathSpec ),
                Functions.jsonApiCommonSyntax( input, pathSpec ), matcher );
    }


    private void assertJsonExists( Object input, JsonExistsErrorBehavior errorBehavior, Matcher<? super Boolean> matcher ) {
        assertThat( invocationDesc( INVOC_DESC_JSON_EXISTS, input, errorBehavior ), Functions.jsonExists( input, errorBehavior ), matcher );
    }


    private void assertJsonExistsFailed( Object input, JsonExistsErrorBehavior errorBehavior, Matcher<? super Throwable> matcher ) {
        assertFailed( invocationDesc( INVOC_DESC_JSON_EXISTS, input, errorBehavior ), () -> Functions.jsonExists( input, errorBehavior ), matcher );
    }


    private void assertJsonValueAny( Object input, JsonValueEmptyOrErrorBehavior emptyBehavior, Object defaultValueOnEmpty, JsonValueEmptyOrErrorBehavior errorBehavior, Object defaultValueOnError, Matcher<Object> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_VALUE_ANY, input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                Functions.jsonValueAny( input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                matcher );
    }


    private void assertJsonValueAnyFailed( Object input, JsonValueEmptyOrErrorBehavior emptyBehavior, Object defaultValueOnEmpty, JsonValueEmptyOrErrorBehavior errorBehavior, Object defaultValueOnError, Matcher<? super Throwable> matcher ) {
        assertFailed(
                invocationDesc( INVOC_DESC_JSON_VALUE_ANY, input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                () -> Functions.jsonValueAny( input, emptyBehavior, defaultValueOnEmpty, errorBehavior, defaultValueOnError ),
                matcher );
    }


    private void assertJsonQuery( Object input, JsonQueryWrapperBehavior wrapperBehavior, JsonQueryEmptyOrErrorBehavior emptyBehavior, JsonQueryEmptyOrErrorBehavior errorBehavior, Matcher<? super String> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_JSON_QUERY, input, wrapperBehavior, emptyBehavior, errorBehavior ),
                Functions.jsonQuery( input, wrapperBehavior, emptyBehavior, errorBehavior ),
                matcher );
    }


    private void assertJsonQueryFailed( Object input, JsonQueryWrapperBehavior wrapperBehavior, JsonQueryEmptyOrErrorBehavior emptyBehavior, JsonQueryEmptyOrErrorBehavior errorBehavior, Matcher<? super Throwable> matcher ) {
        assertFailed(
                invocationDesc( INVOC_DESC_JSON_QUERY, input, wrapperBehavior, emptyBehavior, errorBehavior ),
                () -> Functions.jsonQuery( input, wrapperBehavior, emptyBehavior, errorBehavior ),
                matcher );
    }


    private void assertJsonize( Object input, Matcher<? super String> matcher ) {
        assertThat( invocationDesc( INVOC_DESC_JSONIZE, input ), Functions.jsonize( input ), matcher );
    }


    private void assertDejsonize( String input, Matcher<Object> matcher ) {
        assertThat( invocationDesc( INVOC_DESC_DEJSONIZE, input ), Functions.dejsonize( input ), matcher );
    }


    private void assertDejsonizeFailed( String input, Matcher<? super Throwable> matcher ) {
        assertFailed( invocationDesc( INVOC_DESC_DEJSONIZE, input ), () -> Functions.dejsonize( input ), matcher );
    }


    private void assertJsonObject( Matcher<? super String> matcher, JsonConstructorNullClause nullClause, Object... kvs ) {
        assertThat( invocationDesc( INVOC_DESC_JSON_OBJECT, nullClause, kvs ), Functions.jsonObject( nullClause, kvs ), matcher );
    }


    private void assertJsonObjectAggAdd( Map map, String k, Object v, JsonConstructorNullClause nullClause, Matcher<? super Map> matcher ) {
        Functions.jsonObjectAggAdd( map, k, v, nullClause );
        assertThat(
                invocationDesc( INVOC_DESC_JSON_OBJECT_AGG_ADD, map, k, v, nullClause ), map, matcher );
    }


    private void assertJsonArray( Matcher<? super String> matcher, JsonConstructorNullClause nullClause, Object... elements ) {
        assertThat( invocationDesc( INVOC_DESC_JSON_ARRAY, nullClause, elements ), Functions.jsonArray( nullClause, elements ), matcher );
    }


    private void assertJsonArrayAggAdd( List list, Object element, JsonConstructorNullClause nullClause, Matcher<? super List> matcher ) {
        Functions.jsonArrayAggAdd( list, element, nullClause );
        assertThat(
                invocationDesc( INVOC_DESC_JSON_ARRAY_AGG_ADD, list, element, nullClause ),
                list, matcher );
    }


    private void assertIsJsonValue( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_VALUE, input ),
                Functions.isJsonValue( input ),
                matcher );
    }


    private void assertIsJsonScalar( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_SCALAR, input ),
                Functions.isJsonScalar( input ),
                matcher );
    }


    private void assertIsJsonArray( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_ARRAY, input ),
                Functions.isJsonArray( input ),
                matcher );
    }


    private void assertIsJsonObject( String input, Matcher<? super Boolean> matcher ) {
        assertThat(
                invocationDesc( INVOC_DESC_IS_JSON_OBJECT, input ),
                Functions.isJsonObject( input ),
                matcher );
    }


    private String invocationDesc( String methodName, Object... args ) {
        return methodName + "(" + String.join( ", ", Arrays.stream( args )
                .map( Objects::toString )
                .collect( Collectors.toList() ) ) + ")";
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
        return new BaseMatcher<Throwable>() {
            @Override
            public boolean matches( Object item ) {
                if ( !(item instanceof Throwable) ) {
                    return false;
                }
                Throwable error = (Throwable) item;
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
    private BaseMatcher<Functions.PathContext> contextMatches( Functions.PathContext expected ) {
        return new BaseMatcher<Functions.PathContext>() {
            @Override
            public boolean matches( Object item ) {
                if ( !(item instanceof Functions.PathContext) ) {
                    return false;
                }
                Functions.PathContext context = (Functions.PathContext) item;
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
