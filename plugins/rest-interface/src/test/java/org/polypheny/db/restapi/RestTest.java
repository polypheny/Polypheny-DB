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

import io.javalin.http.Context;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class RestTest {
    private Rest rest;
    private AlgBuilder algBuilder;
    private RexBuilder rexBuilder;

    private void setupMocks() {
        algBuilder = mock(AlgBuilder.class);
        rexBuilder = mock(RexBuilder.class);
    }


    @Test
    @DisplayName("Table scans with single table")
    public void tableScansWithSingleTable() {
        setupMocks();
        LogicalTable logicalTable = mock(LogicalTable.class);

        when(logicalTable.getNamespaceName()).thenReturn("namespace");
        when(logicalTable.getName()).thenReturn("table");
        when(algBuilder.relScan(anyString(), anyString())).thenReturn(algBuilder);

        List<LogicalTable> tables = Collections.singletonList(logicalTable);

        Rest rest = new Rest(null, 0, 0);
        rest.tableScans(algBuilder, rexBuilder, tables);

        verify(algBuilder).relScan("namespace", "table");
        verify(algBuilder, never()).join((JoinAlgType) any(), (Iterable<? extends RexNode>) any());
    }


    @Test
    @DisplayName("Table scans with multiple tables")
    public void tableScansWithMultipleTables() {
        setupMocks();
        LogicalTable logicalTable1 = mock(LogicalTable.class);
        LogicalTable logicalTable2 = mock(LogicalTable.class);
        RexLiteral rexLiteral = mock(RexLiteral.class);

        when(logicalTable1.getNamespaceName()).thenReturn("namespace1");
        when(logicalTable1.getName()).thenReturn("table1");
        when(logicalTable2.getNamespaceName()).thenReturn("namespace2");
        when(logicalTable2.getName()).thenReturn("table2");
        when(algBuilder.relScan(anyString(), anyString())).thenReturn(algBuilder);
        when(algBuilder.join((JoinAlgType) any(), (Iterable<? extends RexNode>) any())).thenReturn(algBuilder);
        when(rexBuilder.makeLiteral(true)).thenReturn(rexLiteral);

        List<LogicalTable> tables = Arrays.asList(logicalTable1, logicalTable2);

        Rest rest = new Rest(null, 0, 0);
        rest.tableScans(algBuilder, rexBuilder, tables);

        verify(algBuilder).relScan("namespace1", "table1");
        verify(algBuilder).relScan("namespace2", "table2");
        verify(algBuilder).join(JoinAlgType.INNER, rexBuilder.makeLiteral(true));
    }


    @Test
    @DisplayName("Values column names with non-empty values")
    public void valuesColumnNamesWithNonEmptyValues() {
        rest = new Rest(null, 0, 0);
        RequestColumn requestColumn1 = mock(RequestColumn.class);
        RequestColumn requestColumn2 = mock(RequestColumn.class);
        LogicalColumn column1 = mock(LogicalColumn.class);
        LogicalColumn column2 = mock(LogicalColumn.class);
        Pair<RequestColumn, PolyValue> pair1 = mock(Pair.class);
        Pair<RequestColumn, PolyValue> pair2 = mock(Pair.class);
        when(requestColumn1.getColumn()).thenReturn(column1);
        when(requestColumn2.getColumn()).thenReturn(column2);
        when(column1.getName()).thenReturn("column1");
        when(column2.getName()).thenReturn("column2");
        List<List<Pair<RequestColumn, PolyValue>>> values = mock(List.class);
        List<Pair<RequestColumn, PolyValue>> value = Arrays.asList(pair1, pair2);
        when(pair1.getLeft()).thenReturn(requestColumn1);
        when(pair2.getLeft()).thenReturn(requestColumn2);
        when(values.get(0)).thenReturn(value);

        List<String> result = rest.valuesColumnNames(values);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("column1"));
        assertTrue(result.contains("column2"));
    }


    @Test
    @DisplayName("Values column names with empty values")
    public void valuesColumnNamesWithEmptyValues() {
        rest = new Rest(null, 0, 0);
        List<List<Pair<RequestColumn, PolyValue>>> values = mock(List.class);
        List<Pair<RequestColumn, PolyValue>> value = new ArrayList<>();
        when(values.get(0)).thenReturn(value);

        List<String> result = rest.valuesColumnNames(values);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }


    @Test
    @DisplayName("Values column names with null values")
    public void valuesColumnNamesWithNullValues() {
        rest = new Rest(null, 0, 0);
        List<List<Pair<RequestColumn, PolyValue>>> values = null;

        assertThrows(NullPointerException.class, () -> rest.valuesColumnNames(values));
    }


    @Test
    @DisplayName("Initial projection with non-empty columns")
    public void initialProjectionWithNonEmptyColumns() {
        setupMocks();
        RequestColumn requestColumn1 = mock(RequestColumn.class);
        RequestColumn requestColumn2 = mock(RequestColumn.class);
        AlgNode algNode = mock(AlgNode.class);
        RexIndexRef rexNode1 = mock(RexIndexRef.class);
        RexIndexRef rexNode2 = mock(RexIndexRef.class);

        when(algBuilder.peek()).thenReturn(algNode);
        when(rexBuilder.makeInputRef(algNode, requestColumn1.getScanIndex())).thenReturn(rexNode1);
        when(rexBuilder.makeInputRef(algNode, requestColumn2.getScanIndex())).thenReturn(rexNode2);
        when(algBuilder.project(Arrays.asList(rexNode1, rexNode2))).thenReturn(algBuilder);

        List<RequestColumn> columns = Arrays.asList(requestColumn1, requestColumn2);

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.initialProjection(algBuilder, rexBuilder, columns);

        verify(algBuilder).project(anyList());
    }


    @Test
    @DisplayName("Initial projection with empty columns")
    public void initialProjectionWithEmptyColumns() {
        setupMocks();
        List<RequestColumn> columns = new ArrayList<>();

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.initialProjection(algBuilder, rexBuilder, columns);

        verify(algBuilder).project(Collections.emptyList());
    }


    @Test
    @DisplayName("Initial projection with null columns")
    public void initialProjectionWithNullColumns() {
        setupMocks();
        Rest rest = new Rest(null, 0, 0);

        assertThrows(NullPointerException.class, () -> rest.initialProjection(algBuilder, rexBuilder, null));
    }


    @Test
    @DisplayName("Final projection with explicit columns")
    public void finalProjectionWithExplicitColumns() {
        setupMocks();
        RequestColumn requestColumn1 = mock(RequestColumn.class);
        RequestColumn requestColumn2 = mock(RequestColumn.class);
        AlgNode algNode = mock(AlgNode.class);
        RexIndexRef rexNode1 = mock(RexIndexRef.class);
        RexIndexRef rexNode2 = mock(RexIndexRef.class);

        when(algBuilder.peek()).thenReturn(algNode);
        when(rexBuilder.makeInputRef(algNode, requestColumn1.getLogicalIndex())).thenReturn(rexNode1);
        when(rexBuilder.makeInputRef(algNode, requestColumn2.getLogicalIndex())).thenReturn(rexNode2);
        when(requestColumn1.isExplicit()).thenReturn(true);
        when(requestColumn2.isExplicit()).thenReturn(true);
        when(algBuilder.project(Arrays.asList(rexNode1, rexNode2), Arrays.asList("alias1", "alias2"), true)).thenReturn(algBuilder);

        List<RequestColumn> columns = Arrays.asList(requestColumn1, requestColumn2);

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.finalProjection(algBuilder, rexBuilder, columns);

        verify(algBuilder).project(anyList(), anyList(), eq(true));
    }


    @Test
    @DisplayName("Final projection with non-explicit columns")
    public void finalProjectionWithNonExplicitColumns() {
        setupMocks();
        RequestColumn requestColumn1 = mock(RequestColumn.class);
        RequestColumn requestColumn2 = mock(RequestColumn.class);

        when(requestColumn1.isExplicit()).thenReturn(false);
        when(requestColumn2.isExplicit()).thenReturn(false);
        when(algBuilder.project(Collections.emptyList(), Collections.emptyList(), true)).thenReturn(algBuilder);

        List<RequestColumn> columns = Arrays.asList(requestColumn1, requestColumn2);

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.finalProjection(algBuilder, rexBuilder, columns);

        verify(algBuilder).project(Collections.emptyList(), Collections.emptyList(), true);
        assertEquals(algBuilder, result);
    }


    @Test
    @DisplayName("Final projection with empty columns")
    public void finalProjectionWithEmptyColumns() {
        setupMocks();
        List<RequestColumn> columns = new ArrayList<>();

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.finalProjection(algBuilder, rexBuilder, columns);

        verify(algBuilder).project(Collections.emptyList(), Collections.emptyList(), true);
    }


    @Test
    @DisplayName("Final projection with null columns")
    public void finalProjectionWithNullColumns() {
        setupMocks();
        Rest rest = new Rest(null, 0, 0);

        assertThrows(NullPointerException.class, () -> rest.finalProjection(algBuilder, rexBuilder, null));
    }


    @Test
    @DisplayName("Aggregates with empty request columns and groupings")
    public void aggregatesWithEmptyRequestColumnsAndGroupings() {
        setupMocks();
        List<RequestColumn> requestColumns = new ArrayList<>();
        List<RequestColumn> groupings = new ArrayList<>();

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.aggregates(algBuilder, rexBuilder, requestColumns, groupings);

        verify(algBuilder, never()).aggregate((AlgBuilder.GroupKey) any(), (AlgBuilder.AggCall) any());
        assertEquals(algBuilder, result);
    }


    @Test
    @DisplayName("Aggregates with null request columns and groupings")
    public void aggregatesWithNullRequestColumnsAndGroupings() {
        setupMocks();
        Rest rest = new Rest(null, 0, 0);

        assertThrows(NullPointerException.class, () -> rest.aggregates(algBuilder, rexBuilder, null, null));
    }


    @Test
    @DisplayName("Sort with null sorts and positive limit and offset")
    public void sortWithNullSortsAndPositiveLimitAndOffset() {
        setupMocks();
        when(algBuilder.limit(anyInt(), anyInt())).thenReturn(algBuilder);

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.sort(algBuilder, rexBuilder, null, 10, 5);

        verify(algBuilder).limit(5, 10);
        assertEquals(algBuilder, result);
    }


    @Test
    @DisplayName("Sort with empty sorts and positive limit and offset")
    public void sortWithEmptySortsAndPositiveLimitAndOffset() {
        setupMocks();
        when(algBuilder.limit(anyInt(), anyInt())).thenReturn(algBuilder);

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.sort(algBuilder, rexBuilder, new ArrayList<>(), 10, 5);

        verify(algBuilder).limit(5, 10);
        assertEquals(algBuilder, result);
    }


    @Test
    @DisplayName("Sort with non-empty sorts")
    public void sortWithNonEmptySorts() {
        setupMocks();
        RequestColumn requestColumn = mock(RequestColumn.class);
        AlgNode algNode = mock(AlgNode.class);
        RexIndexRef inputRef = mock(RexIndexRef.class);
        RexNode innerNode = mock(RexNode.class);
        RexNode sortingNode = mock(RexNode.class);
        when(algBuilder.peek()).thenReturn(algNode);
        when(requestColumn.getLogicalIndex()).thenReturn(0);
        when(rexBuilder.makeInputRef(algNode, 0)).thenReturn(inputRef);
        when(rexBuilder.makeCall(OperatorRegistry.get(OperatorName.DESC), inputRef)).thenReturn(innerNode);
        when(rexBuilder.makeCall(OperatorRegistry.get(OperatorName.NULLS_FIRST), innerNode)).thenReturn(sortingNode);
        when(algBuilder.sortLimit(anyInt(), anyInt(), anyList())).thenReturn(algBuilder);

        List<Pair<RequestColumn, Boolean>> sorts = Arrays.asList(new Pair<>(requestColumn, true));

        Rest rest = new Rest(null, 0, 0);
        AlgBuilder result = rest.sort(algBuilder, rexBuilder, sorts, 10, 5);

        verify(algBuilder).sortLimit(5, 10, Arrays.asList(sortingNode));
        assertEquals(algBuilder, result);
    }


    @Test
    @DisplayName("Execute and transform with query preparation and execution failure")
    public void executeAndTransformWithQueryFailure() {
        AlgRoot algRoot = mock(AlgRoot.class);
        Statement statement = mock(Statement.class);
        Context ctx = mock(Context.class);
        QueryProcessor queryProcessor = mock(QueryProcessor.class);

        when(statement.getQueryProcessor()).thenReturn(queryProcessor);
        when(queryProcessor.prepareQuery(algRoot, true)).thenThrow(new RuntimeException());

        Rest rest = new Rest(null, 0, 0);

        assertThrows(RuntimeException.class, () -> rest.executeAndTransformPolyAlg(algRoot, statement, ctx));
    }
}
