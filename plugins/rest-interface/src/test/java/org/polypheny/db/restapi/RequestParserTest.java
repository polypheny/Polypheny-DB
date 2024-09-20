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

package org.polypheny.db.restapi;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.util.*;
import com.google.gson.Gson;
import io.javalin.http.Context;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.restapi.exception.ParserException;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;

import javax.servlet.http.HttpServletRequest;


public class RequestParserTest extends SqlLanguageDependent {


    private RequestParser requestParser;
    private Catalog catalog;
    private Snapshot snapshot;
    private LogicalRelSnapshot relSnapshot;
    private LogicalTable table;
    private LogicalColumn column;
    private List<LogicalColumn> columns;
    private List<LogicalTable> tables;
    private Map<Long, Integer> tableOffsets;
    private String[] possibleProjectionValues;
    private List<RequestColumn> requestColumns;
    HttpServletRequest request;
    Map<String, String[]> parameterMap;
    Map<String, RequestColumn> nameAndAliasMapping;
    RequestColumn catalogColumn;


    @Test
    @DisplayName("Basic authorization decoding")
    public void basicAuthorizationDecoding() {
        Pair<String, String> unibasDbis = RequestParser.decodeBasicAuthorization( "Basic dW5pYmFzOmRiaXM=" );
        assertEquals( "unibas", unibasDbis.left, "Username was decoded incorrectly." );
        assertEquals( "dbis", unibasDbis.right, "Password was decoded incorrectly." );
    }


    @Test
    @DisplayName("Basic authorization decoding with garbage header")
    public void basicAuthorizationDecodingGarbageHeader() {
        UnauthorizedAccessException thrown = assertThrows( UnauthorizedAccessException.class, () -> {
            Pair<String, String> unibasDbis = RequestParser.decodeBasicAuthorization( "Basic dW5pY!mFzOmRi!" );
            assertEquals( "unibas", unibasDbis.left, "Username was decoded incorrectly." );
            assertEquals( "dbis", unibasDbis.right, "Password was decoded incorrectly." );
        } );
        assertEquals( "Basic Authorization header is not properly encoded.", thrown.getMessage() );

    }


    private void setupMocksForParseCatalogTableName() {
        catalog = mock(Catalog.class);
        snapshot = mock(Snapshot.class);
        relSnapshot = mock(LogicalRelSnapshot.class);
        table = mock(LogicalTable.class);
        requestParser = new RequestParser(catalog, null, null, "username", "testdb");
    }


    @Test
    @DisplayName("Parse catalog table name with valid input")
    public void parseCatalogTableNameWithValidInput() {
        setupMocksForParseCatalogTableName();
        when( catalog.getSnapshot() ).thenReturn( snapshot );
        when( snapshot.rel() ).thenReturn( relSnapshot );

        when( relSnapshot.getTable( "schema1", "table1" ) ).thenReturn( Optional.of( table ) );
        assertDoesNotThrow(() -> requestParser.parseCatalogTableName( "schema1.table1." ));
        verify( snapshot ).rel(); // check if the snapshot was called
        verify( relSnapshot ).getTable( "schema1", "table1" ); // check if the table was called
    }


    @Test
    @DisplayName("Parse catalog table name with non-existent table")
    public void parseCatalogTableNameWithNonExistentTable() {
        setupMocksForParseCatalogTableName();
        when(catalog.getSnapshot()).thenReturn(snapshot);
        when(snapshot.rel()).thenReturn(relSnapshot);
        when(relSnapshot.getTable("schema1", "nonExistentTable")).thenReturn(Optional.empty());

        assertThrows(NoSuchElementException.class, () -> requestParser.parseCatalogTableName("schema1.nonExistentTable"));
    }


    @Test
    @DisplayName("Parse catalog table name with invalid input")
    public void parseCatalogTableNameWithInvalidInput() {
        setupMocksForParseCatalogTableName();
        assertThrows(ParserException.class, () -> requestParser.parseCatalogTableName("invalidFormat"));
    }


    @Test
    @DisplayName("Parse filter operation")
    public void parseFilterOperation() {
        RequestParser requestParser = new RequestParser(
                null,
                null,
                "username",
                "testdb" );
        HashMap<String, Pair<Operator, String>> operationMap = new HashMap<>();
        operationMap.put( ">=10", new Pair<>( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), "10" ) );
        operationMap.put( ">10", new Pair<>( OperatorRegistry.get( OperatorName.GREATER_THAN ), "10" ) );
        operationMap.put( "<=10", new Pair<>( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), "10" ) );
        operationMap.put( "<10", new Pair<>( OperatorRegistry.get( OperatorName.LESS_THAN ), "10" ) );
        operationMap.put( "=10", new Pair<>( OperatorRegistry.get( OperatorName.EQUALS ), "10" ) );
        operationMap.put( "!=10", new Pair<>( OperatorRegistry.get( OperatorName.NOT_EQUALS ), "10" ) );
        operationMap.put( "%10", new Pair<>( OperatorRegistry.get( OperatorName.LIKE ), "10" ) );
        operationMap.put( "!%10", new Pair<>( OperatorRegistry.get( OperatorName.NOT_LIKE ), "10" ) );

        operationMap.forEach( ( k, v ) -> {
            Pair<Operator, String> operationPair = requestParser.parseFilterOperation( k );
            assertEquals( v, operationPair );
        } );

        assertThrows( ParserException.class, () -> requestParser.parseFilterOperation( "invalidOperation" ) );
    }


    private void setupMocksForParseTables() {
        catalog = mock(Catalog.class);
        snapshot = mock(Snapshot.class);
        relSnapshot = mock(LogicalRelSnapshot.class);
        table = mock(LogicalTable.class);
        requestParser = new RequestParser(catalog, null, null, "username", "testdb");
    }

    @Test
    @DisplayName("Test parse tables with valid input")
    public void parseTablesWithValidInput() throws ParserException {
        setupMocksForParseTables();
        when(catalog.getSnapshot()).thenReturn(snapshot);
        when(snapshot.rel()).thenReturn(relSnapshot);
        when(relSnapshot.getTable("schema1", "table1")).thenReturn(Optional.of(table));

        var result = requestParser.parseTables("schema1.table1");

        assertEquals(1, result.size());
        assertEquals(table, result.get(0));
    }


    @Test
    @DisplayName("Test parse tables with multiple valid inputs")
    public void parseTablesWithMultipleValidInputs() throws ParserException {
        setupMocksForParseTables();
        when(catalog.getSnapshot()).thenReturn(snapshot);
        when(snapshot.rel()).thenReturn(relSnapshot);
        when(relSnapshot.getTable("schema1", "table1")).thenReturn(Optional.of(table));
        when(relSnapshot.getTable("schema2", "table2")).thenReturn(Optional.of(table));

        var result = requestParser.parseTables("schema1.table1,schema2.table2");

        assertEquals(2, result.size());
        assertEquals(table, result.get(0));
        assertEquals(table, result.get(1));
    }


    @Test
    @DisplayName("Test parse tables with null input")
    public void parseTablesWithNullInput() {
        setupMocksForParseTables();
        assertThrows(ParserException.class, () -> requestParser.parseTables(null));
    }


    @Test
    @DisplayName("Test parse tables with invalid input")
    public void parseTablesWithInvalidInput() {
        setupMocksForParseTables();
        when(catalog.getSnapshot()).thenReturn(snapshot);
        when(snapshot.rel()).thenReturn(relSnapshot);
        when(relSnapshot.getTable("schema1", "table1")).thenReturn(Optional.empty());

        assertThrows(NoSuchElementException.class, () -> requestParser.parseTables("schema1.table1"));
    }
    

    @Test
    @DisplayName("Generate request columns without project with empty tables")
    public void generateRequestColumnsWithoutProjectWithEmptyTables() {
        catalog = mock(Catalog.class);
        requestParser = new RequestParser(catalog, null, null, "username", "testdb");
        Map<Long, Integer> tableOffsets = new HashMap<>();
        tableOffsets.put(0L, 0);

        List<LogicalTable> tables = new ArrayList<>();

        List<RequestColumn> result = requestParser.generateRequestColumnsWithoutProject(tables, tableOffsets);

        assertEquals(0, result.size());
    }


    private void setupMocksForGenerateRequestColumnsWithProject() {
        requestParser = new RequestParser(catalog, null, null, "username", "testdb");
        snapshot = mock(Snapshot.class);
        relSnapshot = mock(LogicalRelSnapshot.class);
        column = mock(LogicalColumn.class);
    }


    @Test
    @DisplayName("Generate request columns with project with valid input")
    public void generateRequestColumnsWithProjectWithValidInput() throws ParserException {
        setupMocksForGenerateRequestColumnsWithProject();
        Map<Long, Integer> tableOffsets = new HashMap<>();
        tableOffsets.put(1L, 0);

        Set<Long> validColumns = new HashSet<>();
        validColumns.add(1L);

        try(MockedStatic<Catalog> catalogMockedStatic = Mockito.mockStatic(Catalog.class)) {
            catalogMockedStatic.when(Catalog::snapshot).thenReturn(snapshot);
            LogicalNamespace namespace = new LogicalNamespace(1L, "public", DataModel.RELATIONAL, false);
            when(snapshot.getNamespace(anyString())).thenReturn(Optional.of(namespace));
            when(snapshot.rel()).thenReturn(relSnapshot);
            when(relSnapshot.getColumn(namespace.id, "emp", "age")).thenReturn(Optional.of(column));
            when(column.getId()).thenReturn(1L);
            when(column.getTableId()).thenReturn(1L);
            when(column.getPosition()).thenReturn(0);

            List<RequestColumn> result;
            result = requestParser.generateRequestColumnsWithProject(
                    "public.emp.age", tableOffsets, validColumns);

            assertEquals(1, result.size());
            assertEquals(column, result.get(0).getColumn());

            result = requestParser.generateRequestColumnsWithProject(
                    "public.emp.age@alias", tableOffsets, validColumns);

            assertEquals(1, result.size());
            assertEquals(column, result.get(0).getColumn());

            result = requestParser.generateRequestColumnsWithProject(
                    "public.emp.age@alias(SUM)", tableOffsets, validColumns);

            assertEquals(1, result.size());
            assertEquals(column, result.get(0).getColumn());
        }
    }


    @Test
    @DisplayName("Generates name mapping for given request columns")
    public void newGenerateNameMappingForGivenColumns() {
        requestParser = new RequestParser(null, null, null, "username", "testdb");
        requestColumns = new ArrayList<>();
        String[] fullyQualifiedNames = new String[] {"public.emp.age", "public.emp.gender@alias", "public.emp.salary@alias(SUM)"};
        String[] aliases = new String[] {"", "alias", "alias"};
        for(int i = 0; i < 3; i++) {
            RequestColumn requestColumn = mock(RequestColumn.class);
            when(requestColumn.getFullyQualifiedName()).thenReturn(fullyQualifiedNames[i]);
            when(requestColumn.getAlias()).thenReturn(aliases[i]);
            requestColumns.add(requestColumn);
        }

        Map<String, RequestColumn> result = requestParser.newGenerateNameMapping(requestColumns);

        assertEquals(5, result.size());
    }


    @Test
    @DisplayName("Generates empty name mapping for empty request columns")
    public void newGenerateNameMappingForEmptyColumns() {
        requestParser = new RequestParser(null, null, null, "username", "testdb");
        requestColumns = new ArrayList<>();
        List<RequestColumn> requestColumns = new ArrayList<>();

        Map<String, RequestColumn> result = requestParser.newGenerateNameMapping(requestColumns);

        assertTrue(result.isEmpty());
    }


    @Test
    @DisplayName("Decodes aggregate function correctly")
    public void decodeAggregateFunctionWithValidInput() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");

        AggFunction result = requestParser.decodeAggregateFunction("COUNT");
        assertEquals(OperatorRegistry.getAgg(OperatorName.COUNT), result);

        result = requestParser.decodeAggregateFunction("MAX");
        assertEquals(OperatorRegistry.getAgg(OperatorName.MAX), result);

        result = requestParser.decodeAggregateFunction("MIN");
        assertEquals(OperatorRegistry.getAgg(OperatorName.MIN), result);

        result = requestParser.decodeAggregateFunction("AVG");
        assertEquals(OperatorRegistry.getAgg(OperatorName.AVG), result);

        result = requestParser.decodeAggregateFunction("SUM");
        assertEquals(OperatorRegistry.getAgg(OperatorName.SUM), result);
    }


    @Test
    @DisplayName("Returns null for null input")
    public void decodeAggregateFunctionWithNullInput() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");

        AggFunction result = requestParser.decodeAggregateFunction(null);
        assertNull(result);
    }


    @Test
    @DisplayName("Returns null for unknown aggregate function")
    public void decodeAggregateFunctionWithUnknownInput() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");

        AggFunction result = requestParser.decodeAggregateFunction("UNKNOWN");
        assertNull(result);
    }


    private void setupMockForParserSorting() {
        request = mock(HttpServletRequest.class);
        parameterMap = new HashMap<>();
        when(request.getParameterMap()).thenReturn(parameterMap);

        nameAndAliasMapping = mock(Map.class);
        catalogColumn = mock(RequestColumn.class);
        when(nameAndAliasMapping.get(anyString())).thenReturn(catalogColumn);
    }


    @Test
    @DisplayName("Parses sorting with valid input")
    public void parseSortingWithValidInput() throws ParserException {
        setupMockForParserSorting();
        parameterMap.put("_sort", new String[]{"column1@DESC"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        List<Pair<RequestColumn, Boolean>> result = requestParser.parseSorting(request, nameAndAliasMapping);

        assertEquals(1, result.size());
        assertEquals(catalogColumn, result.get(0).left);
        assertTrue(result.get(0).right);
    }


    @Test
    @DisplayName("Parses sorting with no sort parameter")
    public void parseSortingWithNoSortParameter() throws ParserException {
        setupMockForParserSorting();
        when(request.getParameterMap()).thenReturn(parameterMap);

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        List<Pair<RequestColumn, Boolean>> result = requestParser.parseSorting(request, nameAndAliasMapping);

        assertNull(result);
    }


    @Test
    @DisplayName("Throws exception for malformed sort instruction")
    public void parseSortingThrowsExceptionForMalformedSortInstruction() {
        setupMockForParserSorting();
        parameterMap.put("_sort", new String[]{"column1, column2@DESC"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        ParserException exception = assertThrows(ParserException.class, () -> requestParser.parseSorting(request, nameAndAliasMapping));
        assertEquals(ParserErrorCode.SORT_MALFORMED, exception.getErrorCode());
    }


    private void setupMockForParseFunctions() {
        request = mock(HttpServletRequest.class);
        parameterMap = new HashMap<>();
        when(request.getParameterMap()).thenReturn(parameterMap);
    }


    @Test
    @DisplayName("Parse groupings with valid input")
    public void parseGroupingsWithValidInput() throws ParserException {
        setupMockForParseFunctions();
        parameterMap.put("_groupby", new String[]{"column1,column2"});

        Map<String, RequestColumn> nameAndAliasMapping = new HashMap<>();
        RequestColumn column1 = mock(RequestColumn.class);
        RequestColumn column2 = mock(RequestColumn.class);
        nameAndAliasMapping.put("column1", column1);
        nameAndAliasMapping.put("column2", column2);

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        List<RequestColumn> result = requestParser.parseGroupings(request, nameAndAliasMapping);

        assertEquals(2, result.size());
        assertTrue(result.contains(column1));
        assertTrue(result.contains(column2));
    }


    @Test
    @DisplayName("Parse groupings with no groupby parameter")
    public void parseGroupingsWithNoGroupbyParameter() throws ParserException {
        setupMockForParseFunctions();

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        List<RequestColumn> result = requestParser.parseGroupings(request, new HashMap<>());

        assertTrue(result.isEmpty());
    }


    @Test
    @DisplayName("Parse groupings with unknown column")
    public void parseGroupingsWithUnknownColumn() {
        setupMockForParseFunctions();
        parameterMap.put("_groupby", new String[]{"unknownColumn"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        ParserException exception = assertThrows(ParserException.class, () -> requestParser.parseGroupings(request, new HashMap<>()));
        assertEquals(ParserErrorCode.GROUPING_UNKNOWN, exception.getErrorCode());
    }


    @Test
    @DisplayName("Parse limit with valid limit")
    public void parseLimitWithValidLimit() throws ParserException {
        setupMockForParseFunctions();
        parameterMap.put("_limit", new String[]{"10"});


        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        Integer result = requestParser.parseLimit(request);

        assertEquals(10, result);
    }


    @Test
    @DisplayName("Parse limit with no limit parameter")
    public void parseLimitWithNoLimitParameter() throws ParserException {
        setupMockForParseFunctions();

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        Integer result = requestParser.parseLimit(request);

        assertEquals(-1, result);
    }

    @Test
    @DisplayName("Parse limit with non-numeric limit")
    public void parseLimitWithNonNumericLimit() {
        setupMockForParseFunctions();
        parameterMap.put("_limit", new String[]{"nonNumeric"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        ParserException exception = assertThrows(ParserException.class, () -> requestParser.parseLimit(request));
        assertEquals(ParserErrorCode.LIMIT_MALFORMED, exception.getErrorCode());
    }


    @Test
    @DisplayName("Parse offset with valid offset")
    public void parseRequestOffsetWithValidOffset() throws ParserException {
        setupMockForParseFunctions();
        parameterMap.put("_offset", new String[]{"10"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        Integer result = requestParser.parseOffset(request);

        assertEquals(10, result);
    }


    @Test
    @DisplayName("Parse offset with no offset parameter")
    public void parseRequestOffsetWithNoOffsetParameter() throws ParserException {
        setupMockForParseFunctions();

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        Integer result = requestParser.parseOffset(request);

        assertEquals(-1, result);
    }

    @Test
    @DisplayName("Parse offset with non-numeric offset")
    public void parseRequestOffsetWithNonNumericOffset() {
        setupMockForParseFunctions();
        parameterMap.put("_offset", new String[]{"nonNumeric"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        ParserException exception = assertThrows(ParserException.class, () -> requestParser.parseOffset(request));
        assertEquals(ParserErrorCode.OFFSET_MALFORMED, exception.getErrorCode());
    }


    @Test
    @DisplayName("Parse offset with valid offset")
    public void parseStringOffsetWithValidOffset() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        Integer result = requestParser.parseOffset("10");
        assertEquals(10, result);
    }


    @Test
    @DisplayName("Parse offset with negative offset")
    public void parseStringOffsetWithNegativeOffset() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        Integer result = requestParser.parseOffset("-10");
        assertEquals(-10, result);
    }


    @Test
    @DisplayName("Parse offset with non-numeric offset")
    public void parseStringOffsetWithNonNumericOffset() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        ParserException exception = assertThrows(ParserException.class, () -> requestParser.parseOffset("nonNumeric"));
        assertEquals(ParserErrorCode.OFFSET_MALFORMED, exception.getErrorCode());
    }


    @Test
    @DisplayName("Parse filters with valid input")
    public void parseFiltersWithValidInput() throws ParserException {
        Map<String, String[]> filterMap = new HashMap<>();
        filterMap.put("column1", new String[]{"=10"});
        filterMap.put("column2", new String[]{"<20"});

        RequestColumn column1 = mock(RequestColumn.class);
        RequestColumn column2 = mock(RequestColumn.class);
        LogicalColumn logicalColumn1 = mock(LogicalColumn.class);
        LogicalColumn logicalColumn2 = mock(LogicalColumn.class);
        when(column1.getColumn()).thenReturn(logicalColumn1);
        when(column2.getColumn()).thenReturn(logicalColumn2);
        when(logicalColumn1.getType()).thenReturn(PolyType.INTEGER);
        when(logicalColumn2.getType()).thenReturn(PolyType.INTEGER);

        Map<String, RequestColumn> nameAndAliasMapping = new HashMap<>();
        nameAndAliasMapping.put("column1", column1);
        nameAndAliasMapping.put("column2", column2);

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        RequestParser.Filters result = requestParser.parseFilters(filterMap, nameAndAliasMapping);

        assertEquals(2, result.literalFilters.size());
    }


    @Test
    @DisplayName("Parse filters with unknown column")
    public void parseFiltersWithUnknownColumn() {
        Map<String, String[]> filterMap = new HashMap<>();
        filterMap.put("unknownColumn", new String[]{"=10"});

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        ParserException exception = assertThrows(ParserException.class, () -> requestParser.parseFilters(filterMap, new HashMap<>()));
        assertEquals(ParserErrorCode.FILTER_UNKNOWN_COLUMN, exception.getErrorCode());
    }


    @Test
    @DisplayName("Parse filters with no filters")
    public void parseFiltersWithNoFilters() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        RequestParser.Filters result = requestParser.parseFilters(new HashMap<>(), new HashMap<>());

        assertTrue(result.literalFilters.isEmpty());
        assertTrue(result.columnFilters.isEmpty());
    }


    @Test
    @DisplayName("Parse literal value with valid boolean input")
    public void parseLiteralValueWithValidBooleanInput() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        PolyValue result = requestParser.parseLiteralValue(PolyType.BOOLEAN, "true");
        assertEquals(PolyBoolean.of(true), result);
    }


    @Test
    @DisplayName("Parse literal value with valid integer input")
    public void parseLiteralValueWithValidIntegerInput() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        PolyValue result = requestParser.parseLiteralValue(PolyType.INTEGER, "10");
        assertEquals(PolyBigDecimal.of(new BigDecimal("10")), result);
    }


    @Test
    @DisplayName("Parse literal value with valid decimal input")
    public void parseLiteralValueWithValidDecimalInput() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        PolyValue result = requestParser.parseLiteralValue(PolyType.DECIMAL, "10.5");
        assertEquals(PolyBigDecimal.of(new BigDecimal("10.5")), result);
    }


    @Test
    @DisplayName("Parse literal value with valid string input")
    public void parseLiteralValueWithValidStringInput() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        PolyValue result = requestParser.parseLiteralValue(PolyType.VARCHAR, "test");
        assertEquals(PolyString.of("test"), result);
    }


    @Test
    @DisplayName("Parse literal value with valid date input")
    public void parseLiteralValueWithValidDateInput() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        PolyValue result = requestParser.parseLiteralValue(PolyType.DATE, "2022-01-01");
        assertEquals(PolyDate.of(new DateString("2022-01-01").getMillisSinceEpoch()), result);
    }


    @Test
    @DisplayName("Parse literal value with invalid input")
    public void parseLiteralValueWithInvalidInput() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        NumberFormatException exception = assertThrows(NumberFormatException.class, () -> requestParser.parseLiteralValue(PolyType.INTEGER, "invalid"));
    }


    @Test
    @DisplayName("Parse literal value with unsupported type")
    public void parseLiteralValueWithUnsupportedType() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        assertThrows(NotImplementedException.class, () -> requestParser.parseLiteralValue(PolyType.CHAR, new Object()));
    }


    @Test
    @DisplayName("Parse values with missing values statement")
    public void parseValuesWithMissingValuesStatement() {
        Context ctx = mock(Context.class);
        Gson gson = new Gson();
        Map<String, RequestColumn> nameMapping = new HashMap<>();

        Map<String, Object> bodyMap = new HashMap<>();

        when(ctx.bodyAsClass(Object.class)).thenReturn(bodyMap);

        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");
        assertThrows(ParserException.class, () -> requestParser.parseValues(ctx, gson, nameMapping));
    }


    @Test
    @DisplayName("Parse insert statement body with empty input")
    public void parseInsertStatementBodyWithEmptyInput() throws ParserException {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");

        List<Object> bodyInsertValues = new ArrayList<>();
        Map<String, RequestColumn> nameMapping = new HashMap<>();

        List<List<Pair<RequestColumn, PolyValue>>> result = requestParser.parseInsertStatementBody(bodyInsertValues, nameMapping);

        assertTrue(result.isEmpty());
    }


    @Test
    @DisplayName("Parse insert statement body with null input")
    public void parseInsertStatementBodyWithNullInput() {
        RequestParser requestParser = new RequestParser(null, null, null, "username", "testdb");

        assertThrows(NullPointerException.class, () -> requestParser.parseInsertStatementBody(null, null));
    }
}
