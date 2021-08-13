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


/**
 * Packaging all the information and algorithm used by
 * {@link org.polypheny.cql.parser.CqlParser} to build {@link CqlQuery}.
 */
public class CqlQueryBuilder {

    private final String databaseName;
    private final Stack<Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter>> filters;
    private final Map<String, TableIndex> tableIndexMapping;
    private final Map<String, TableIndex> tableAliases;
    private final Map<String, ColumnIndex> columnIndexMapping;
    private final List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications;
    private final Projections projections;
    private Tree<Combiner, TableIndex> queryRelation;
    private TableIndex lastTableIndex;


    public CqlQueryBuilder( String databaseName ) {
        this.databaseName = databaseName;
        this.filters = new Stack<>();
        this.tableIndexMapping = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.tableAliases = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.columnIndexMapping = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.sortSpecifications = new ArrayList<>();
        this.projections = new Projections();
    }


    /**
     * Build the {@link CqlQuery} object. To be called after
     * the {@link org.polypheny.cql.parser.CqlParser} has parsed
     * the full query.
     *
     * @return {@link CqlQuery}
     * @throws Exception Thrown if the query specified no relation or filters.
     */
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


    /**
     * Generates a default relation made by of INNER JOIN-ing the tables
     * of all the columns (including columns in filters, sort specifications
     * and projections) used in the query.
     */
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


    /**
     * Creates and adds a {@link TableIndex} as represented by the
     * input parameter.
     *
     * @param fullyQualifiedTableName Expected format: SCHEMA_NAME.TABLE_NAME.
     * @return {@link TableIndex}.
     * @throws UnknownIndexException Thrown if the {@link org.polypheny.db.catalog.Catalog}
     * does not contain the table as specified by the input parameter.
     */
    public TableIndex addTableIndex( String fullyQualifiedTableName )
            throws UnknownIndexException {

        String[] split = fullyQualifiedTableName.split( "\\." );

        assert split.length == 2;

        return addTableIndex( split[0], split[1] );
    }


    /**
     * Creates and adds a {@link TableIndex} as represented by the
     * input parameters.
     *
     * @return {@link TableIndex}.
     * @throws UnknownIndexException Thrown if the {@link org.polypheny.db.catalog.Catalog}
     * does not contain the table as specified by the input parameters.
     */
    public TableIndex addTableIndex( String schemaName, String tableName ) throws UnknownIndexException {

        String fullyQualifiedTableName = schemaName + "." + tableName;

        if ( !this.tableIndexMapping.containsKey( fullyQualifiedTableName ) ) {
            TableIndex tableIndex = TableIndex.createIndex( databaseName, schemaName, tableName );
            this.tableIndexMapping.put( tableIndex.fullyQualifiedName, tableIndex );
        }
        return this.tableIndexMapping.get( fullyQualifiedTableName );
    }


    /**
     * Creates and adds a {@link ColumnIndex} as represented by the
     * input parameter. It also adds the {@link TableIndex} of the
     * table that the column belongs to.
     *
     * @param fullyQualifiedColumnName Expected format: SCHEMA_NAME.TABLE_NAME.COLUMN_NAME.
     * @return {@link ColumnIndex}.
     * @throws UnknownIndexException Thrown if the {@link org.polypheny.db.catalog.Catalog}
     * does not contain the column as specified by the input parameter.
     */
    public ColumnIndex addColumnIndex( String fullyQualifiedColumnName )
            throws UnknownIndexException {

        String[] split = fullyQualifiedColumnName.split( "\\." );

        assert split.length == 3;

        return addColumnIndex( split[0], split[1], split[2] );
    }


    /**
     * Creates and adds a {@link ColumnIndex} as represented by the
     * input parameter. It also adds the {@link TableIndex} of the
     * table that the column belongs to.
     *
     * @return {@link ColumnIndex}.
     * @throws UnknownIndexException Thrown if the {@link org.polypheny.db.catalog.Catalog}
     * does not contain the column as specified by the input parameter.
     */
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


    /**
     * Adds the first {@link TableIndex} to {@link #queryRelation}.
     * It should only be called once, when adding the first {@link TableIndex}.
     */
    public void addTable( TableIndex tableIndex ) {
        assert this.queryRelation == null;

        this.queryRelation = new Tree<>( tableIndex );
        this.lastTableIndex = tableIndex;
    }


    /**
     * Combines the existing {@link #queryRelation} with {@link TableIndex}
     * using {@link Combiner}. It should only be called after {@link #addTable(TableIndex)}.
     *
     * @param tableIndex table to be combined.
     * @param booleanGroup {@link BooleanGroup<TableOpsBooleanOperator>} to
     * create {@link Combiner}.
     */
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


    /**
     * Creates a {@link Tree} leaf node using the input parameter
     * ({@link Filter}).
     */
    public void addNewFilter( Filter filter ) {
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> root = new Tree<>( filter );
        this.filters.push( root );
    }


    /**
     * Merges the last two added {@link Filter}s using the
     * {@link BooleanGroup<ColumnOpsBooleanOperator>}
     */
    public void mergeFilterSubtreesWith( BooleanGroup<ColumnOpsBooleanOperator> booleanGroup ) {
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> right = this.filters.pop();
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> left = this.filters.pop();

        left = new Tree<>(
                left,
                booleanGroup,
                right
        );

        this.filters.push( left );
    }


    /**
     * Adds to the sort specification list.
     */
    public void addSortSpecification( ColumnIndex columnIndex, Map<String, Modifier> modifiers ) {
        this.sortSpecifications.add(
                new Pair<>( columnIndex, modifiers )
        );
    }


    /**
     * Creates and adds the {@link Projections.Projection}.
     */
    public void addProjection( ColumnIndex columnIndex, Map<String, Modifier> modifiers ) {
        projections.add( columnIndex, modifiers );
    }

}
