/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.cql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.polypheny.cql.BooleanGroup.ColumnOpsBooleanOperator;
import org.polypheny.cql.BooleanGroup.TableOpsBooleanOperator;
import org.polypheny.cql.exception.UnknownIndexException;
import org.polypheny.cql.utils.Tree;
import org.polypheny.db.util.Pair;

public class CqlQueryBuilder {

    private String databaseName;
    private Tree<Combiner, TableIndex> queryRelation;
    private Stack<Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter>> filters;
    private Map<String, TableIndex> tableIndexMapping;
    private Map<String, TableIndex> tableAliases;
    private Map<String, ColumnIndex> columnIndexMapping;
    private List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications;
    private List<Pair<ColumnIndex, Map<String, Modifier>>> projections;

    private TableIndex lastTableIndex;


    public CqlQueryBuilder( String databaseName ) {
        this.databaseName = databaseName;
        this.filters = new Stack<>();
        this.tableIndexMapping = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.tableAliases = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.columnIndexMapping = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.sortSpecifications = new ArrayList<>();
        this.projections = new ArrayList<>();
    }


    public CqlQuery build() throws Exception {
        if ( queryRelation == null && filters.size() == 0 ) {
            throw new Exception( "Query relations and filters cannot be both empty." );
        }

        if ( queryRelation == null ) {
            assert filters.size() == 1;

            generateDefaultQueryRelation();
        }

        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> queryFilters = null;
        if ( filters.size() == 1 ) {
            queryFilters = filters.pop();
        }

        return new CqlQuery( queryRelation, queryFilters, tableIndexMapping,
                columnIndexMapping, sortSpecifications, projections );
    }


    private void generateDefaultQueryRelation() {
        AtomicBoolean first = new AtomicBoolean( true );
        tableIndexMapping.forEach( ( tableName, tableIndex ) -> {
            if ( first.get() ) {
                addTable( tableIndex );
                first.set( false );
            } else {
                BooleanGroup<TableOpsBooleanOperator> innerJoin = new BooleanGroup<>( TableOpsBooleanOperator.AND );
                combineRelationWith( tableIndex, innerJoin );
            }
        } );
    }


    public TableIndex addTableIndex ( String fullyQualifiedTableName )
            throws UnknownIndexException {

        String[] split = fullyQualifiedTableName.split( "\\." );

        assert split.length == 2;

        return addTableIndex( split[0], split[1] );
    }


    public TableIndex addTableIndex( String schemaName, String tableName ) throws UnknownIndexException {

        String fullyQualifiedTableName = schemaName + "." + tableName;

        if ( !this.tableIndexMapping.containsKey( fullyQualifiedTableName ) ) {
            TableIndex tableIndex = TableIndex.createIndex( databaseName, schemaName, tableName );
            this.tableIndexMapping.put( tableIndex.fullyQualifiedName, tableIndex );
        }
        return this.tableIndexMapping.get( fullyQualifiedTableName );
    }


    public ColumnIndex addColumnIndex( String fullyQualifiedColumnName )
            throws UnknownIndexException {

        String[] split = fullyQualifiedColumnName.split( "\\." );

        assert split.length == 3;

        return addColumnIndex( split[0], split[1], split[2] );
    }


    public ColumnIndex addColumnIndex( String schemaName, String tableName, String columnName )
            throws UnknownIndexException {

        addTableIndex( schemaName, tableName );

        String fullyQualifiedColumnName = schemaName + "." + tableName + "." + columnName;
        if ( !columnIndexMapping.containsKey( fullyQualifiedColumnName ) ) {
            ColumnIndex columnIndex = ColumnIndex.createIndex( databaseName, schemaName, tableName, columnName );
            columnIndexMapping.put( columnIndex.fullyQualifiedName, columnIndex );
        }

        return columnIndexMapping.get( fullyQualifiedColumnName );
    }


    public void addTable( TableIndex tableIndex ) {
        assert this.queryRelation == null;

        this.queryRelation = new Tree<>( tableIndex );
        this.lastTableIndex = tableIndex;
    }


    public void combineRelationWith( TableIndex tableIndex, BooleanGroup<TableOpsBooleanOperator> booleanGroup ) {

        assert this.queryRelation != null;

        Combiner combiner = Combiner.createCombiner( booleanGroup, this.lastTableIndex, tableIndex );

        this.queryRelation = new Tree<>(
                this.queryRelation,
                combiner,
                new Tree<>( tableIndex )
        );

        this.lastTableIndex = tableIndex;
    }


    public void addNewFilter( Filter filter ) {
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> root = new Tree<>( filter );
        this.filters.push( root );
    }


    public void mergeFilterSubtreesWith( BooleanGroup<ColumnOpsBooleanOperator> booleanGroup ) {
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> right = this.filters.pop();
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> left = this.filters.pop();
        assert right != null;
        assert left != null;

        left = new Tree<>(
                left,
                booleanGroup,
                right
        );

        this.filters.push( left );
    }

    public void addSortSpecification( ColumnIndex columnIndex, Map<String, Modifier> modifiers ) {
        this.sortSpecifications.add(
                new Pair<>( columnIndex, modifiers )
        );
    }

    public void addProjection( ColumnIndex columnIndex, Map<String, Modifier> modifiers ) {
        this.projections.add(
                new Pair<>( columnIndex, modifiers )
        );
    }
}
