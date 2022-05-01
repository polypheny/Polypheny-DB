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

package org.polypheny.db.adaptimizer.randomtrees;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.Builder;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class RandomTreeTemplateBuilder implements Builder<RelRandomTreeTemplate> {


    private final Catalog catalog;


    private static final float DEFAULT_UNARY_PROBABILITY = 0.5f;
    private static final List<String> DEFAULT_UNARY_OPERATORS = List.of( "Project", "Sort", "Filter" );
    private static final List<String> DEFAULT_BINARY_OPERATORS = List.of( "Join", "Union", "Intersect", "Minus" );
    private static final boolean DEFAULT_SWITCH = false;
    private static final String DEFAULT_SCHEMA_NAME = "adapt";
    private static final int DEFAULT_HEIGHT = 4;
    private static final List<JoinAlgType> DEFAULT_JOIN_TYPES = List.of( JoinAlgType.LEFT, JoinAlgType.RIGHT, JoinAlgType.FULL, JoinAlgType.INNER);
    private static final long DEFAULT_SEED = 1337;
    private static final List<String> DEFAULT_FILTER_OPERATIONS = List.of( "<>" );
    private static final List<String> DEFAULT_JOIN_OPERATIONS = List.of( "=" );
    private static final List<Boolean> DEFAULT_SET_OP_ALL = List.of( true );

    HashMap<String, PolyType> columnTypes = new HashMap<>();
    List<TableRecord> tableRecords = new LinkedList<>();
    Set<Pair<String, String>> references = new HashSet<>();
    Set<Pair<String, String>> polyTypePartners = new HashSet<>();

    private final List<String> binaryOperators;
    private final List<String> unaryOperators;
    private final List<JoinAlgType> joinTypes;
    private final List<String> filterOps;
    private final List<String> joinOps;

    private final List<CatalogTable> catalogTables;
    private String schemaName;

    private Integer maxHeight;
    private Long seed;
    private Float unaryProbability;
    private Boolean switchFlag;

    public RandomTreeTemplateBuilder( Catalog catalog ) {
        this.catalog = catalog;
        this.binaryOperators = new LinkedList<>();
        this.joinTypes = new LinkedList<>();
        this.unaryOperators = new LinkedList<>();
        this.catalogTables = new LinkedList<>();
        this.filterOps = new LinkedList<>();
        this.joinOps = new LinkedList<>();
    }

    public RandomTreeTemplateBuilder addTable( CatalogTable catalogTable ) {
        this.catalogTables.add( catalogTable );
        return this;
    }

    public RandomTreeTemplateBuilder toggleSwitch() {
        this.switchFlag = ! this.switchFlag;
        return this;
    }

    public RandomTreeTemplateBuilder setUnaryProbability( float unaryProbability ) {
        this.unaryProbability = unaryProbability;
        return this;
    }


    public RandomTreeTemplateBuilder setSeed( long seed ) {
        this.seed = seed;
        return this;
    }


    public RandomTreeTemplateBuilder addUnaryOperator( String operator, int frequency ) {
        if ( frequency > 0 ) {
            this.addUnaryOperator( operator, frequency - 1 );
        }
        this.unaryOperators.add( operator );
        return this;
    }

    public RandomTreeTemplateBuilder addBinaryOperator( String operator, int frequency ) {
        if ( frequency > 0 ) {
            this.addBinaryOperator( operator, frequency - 1 );
        }
        this.binaryOperators.add( operator );
        return this;
    }

    public RandomTreeTemplateBuilder addJoinType( JoinAlgType joinAlgType, int frequency ) {
        if ( frequency > 0 ) {
            this.addJoinType( joinAlgType, frequency - 1 );
        }
        this.joinTypes.add( joinAlgType );
        return this;
    }

    public RandomTreeTemplateBuilder addFilterOp( String string ) {
        this.filterOps.add( string );
        return this;
    }


    public RandomTreeTemplateBuilder setMaxHeight( int height ) {
        this.maxHeight = height;
        return this;
    }

    public RandomTreeTemplateBuilder setSchemaName( String schemaName ) {
        this.schemaName = schemaName;
        return this;
    }

    @Override
    public RelRandomTreeTemplate build() {

        // Parse tables and map column-names to column-types
        HashMap<String, PolyType> columnTypes = new HashMap<>();
        List<TableRecord> tableRecords = new LinkedList<>();

        for ( CatalogTable catalogTable : this.catalogTables ) {
            List<CatalogColumn> catalogColumns = catalogTable.columnIds.stream().map( catalog::getColumn ).collect( Collectors.toList());
            catalogColumns.forEach( catalogColumn -> columnTypes.put( catalogTable.name + "." + catalogColumn.name, catalogColumn.type ) );
            List<String> columns = catalogTable.columnIds.stream().map( catalog::getColumn ).map( c -> catalogTable.name + "." + c.name ).collect( Collectors.toList());
            tableRecords.add( new AdaptiveTableRecord( catalogTable.name, columns ) );
        }

        // Parse tables again for all foreign key references...
        Set<Pair<String, String>> references = new HashSet<>();

        for ( CatalogTable catalogTableA : this.catalogTables ) {
            List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( catalogTableA.id );

            for ( CatalogForeignKey foreignKey : foreignKeys ) {
                List<CatalogColumn> columns = foreignKey.columnIds.stream().map( catalog::getColumn ).collect( Collectors.toList());
                List<CatalogColumn> referencedColumns = foreignKey.referencedKeyColumnIds.stream().map( catalog::getColumn ).collect( Collectors.toList());

                CatalogTable catalogTableB = catalog.getTable( foreignKey.referencedKeyTableId );

                for ( int i = 0; i < columns.size(); i++ ) {
                    references.add( new Pair<>( catalogTableA.name + "." + columns.get( i ).name, catalogTableB.name + "." + referencedColumns.get( i ).name ) );
                    references.add( new Pair<>( catalogTableB.name + "." + referencedColumns.get( i ).name, catalogTableA.name + "." + columns.get( i ).name ) );
                }
            }
        }

        // Parse tables again for all tuples of matching PolyTypes...
        Set<Pair<String, String>> polyTypePartners = new HashSet<>();

        for ( CatalogTable catalogTableA : this.catalogTables ) {
            List<CatalogColumn> catalogColumnsA = catalog.getColumns( catalogTableA.id );
            for ( CatalogTable catalogTableB : this.catalogTables ) {
                List<CatalogColumn> catalogColumnsB = catalog.getColumns( catalogTableB.id );

                for ( CatalogColumn catalogColumnA : catalogColumnsA ) {
                    for ( CatalogColumn catalogColumnB : catalogColumnsB ) {
                        if ( ! ( catalogTableA.name + "." + catalogColumnA.name ).equals( catalogTableB.name + "." + catalogColumnB.name ) && catalogColumnA.type == catalogColumnB.type ) {
                            polyTypePartners.add( new Pair<>( catalogTableA.name + "." + catalogColumnA.name, catalogTableB.name + "." + catalogColumnB.name ) );
                        }
                    }
                }
            }
        }


        // Return the template
        return new RelRandomTreeTemplate(
                ( this.schemaName  == null ) ? DEFAULT_SCHEMA_NAME : this.schemaName,
                ( this.maxHeight == null ) ? DEFAULT_HEIGHT : this.maxHeight,
                ( this.seed  == null ) ? DEFAULT_SEED : this.seed,
                ( this.unaryProbability  == null ) ? DEFAULT_UNARY_PROBABILITY : this.unaryProbability,
                ( this.switchFlag  == null ) ? DEFAULT_SWITCH : this.switchFlag,
                ( this.binaryOperators.isEmpty() ) ? DEFAULT_BINARY_OPERATORS : this.binaryOperators,
                ( this.unaryOperators.isEmpty() ) ? DEFAULT_UNARY_OPERATORS : this.unaryOperators,
                ( this.joinTypes.isEmpty() ) ? DEFAULT_JOIN_TYPES : this.joinTypes,
                ( this.filterOps.isEmpty() ) ? DEFAULT_FILTER_OPERATIONS : this.filterOps,
                ( this.joinOps.isEmpty() ) ? DEFAULT_JOIN_OPERATIONS : this.joinOps,
                tableRecords,
                polyTypePartners,
                references,
                columnTypes
        );

    }

}
