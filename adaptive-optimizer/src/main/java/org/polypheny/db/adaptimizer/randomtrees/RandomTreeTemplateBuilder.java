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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.Builder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class RandomTreeTemplateBuilder implements Builder<RelRandomTreeTemplate> {
    private final Catalog catalog;

    private final List<Pair<String, Integer>> operators;

    private List<CatalogTable> catalogTables;
    private String schemaName;
    private int maxHeight;

    public RandomTreeTemplateBuilder( Catalog catalog ) {
        this.catalog = catalog;
        this.operators = new LinkedList<>();
        this.catalogTables = new LinkedList<>();
    }

    public RandomTreeTemplateBuilder addOperator( String operator, int frequency ) {
        this.operators.add( new Pair<>( operator, frequency ) );
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

    public RandomTreeTemplateBuilder addTable( CatalogTable catalogTable ) {
        this.catalogTables.add( catalogTable );
        return this;
    }

    @Override
    public RelRandomTreeTemplate build() {

        HashMap<String, PolyType> columnTypes;
        Set<Pair<String, String>> references;
        List<AdaptiveTableRecord> tableRecords;
        Set<Pair<String, String>> polyTypePartners;

        columnTypes = new HashMap<>();
        references = new HashSet<>();
        tableRecords = new LinkedList<>();

        for ( CatalogTable catalogTable : this.catalogTables ) {
            List<CatalogColumn> catalogColumns = catalogTable.columnIds.stream().map( catalog::getColumn ).collect( Collectors.toList());
            catalogColumns.forEach( catalogColumn -> columnTypes.put( catalogTable.name + "." + catalogColumn.name, catalogColumn.type ) );
            List<String> columns = catalogTable.columnIds.stream().map( catalog::getColumn ).map( c -> catalogTable.name + "." + c.name ).collect( Collectors.toList());
            tableRecords.add( new AdaptiveTableRecord( catalogTable.name, columns ) );
        }


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

        polyTypePartners = new HashSet<>();

        for ( CatalogTable catalogTableA : this.catalogTables ) {
            List<CatalogColumn> catalogColumnsA = catalog.getColumns( catalogTableA.id );
            for ( CatalogTable catalogTableB : this.catalogTables ) {
                List<CatalogColumn> catalogColumnsB = catalog.getColumns( catalogTableB.id );

                for ( CatalogColumn catalogColumnA : catalogColumnsA ) {
                    for ( CatalogColumn catalogColumnB : catalogColumnsB ) {

                        if ( catalogColumnA != catalogColumnB && catalogColumnA.type == catalogColumnB.type ) {
                            polyTypePartners.add( new Pair<>( catalogColumnA.name + "." + catalogColumnA.name, catalogColumnB.name + "." + catalogColumnB.name ) );
                        }

                    }
                }

            }

        }

        ArrayList<String> operatorDistribution = new ArrayList<>();
        for ( Pair<String, Integer> operator : this.operators ) {
            for ( int i = 0; i < operator.right; i++ ) {
                operatorDistribution.add( operator.left );
            }
        }

        return new RelRandomTreeTemplate(
                this.schemaName,
                columnTypes,
                operatorDistribution,
                this.maxHeight,
                polyTypePartners,
                references,
                tableRecords
        );
    }

}
