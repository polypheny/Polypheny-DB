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

package org.polypheny.db.adaptimizer.rndqueries;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Getter
public abstract class QueryTemplateUtil {

    public static HashMap<String, PolyType> getColumnTypes( Catalog catalog, List<CatalogTable> catalogTables ) {
        HashMap<String, PolyType> columnTypes = new HashMap<>();

        for ( CatalogTable catalogTable : catalogTables ) {
            List<CatalogColumn> catalogColumns = catalogTable.fieldIds.stream().map( catalog::getColumn ).collect( Collectors.toList());
            catalogColumns.forEach( catalogColumn -> columnTypes.put( catalogTable.name + "." + catalogColumn.name, catalogColumn.type ) );
        }

        return columnTypes;
    }


    public static List<AdaptiveTableRecord> getTableRecords( Catalog catalog, List<CatalogTable> catalogTables ) {
        List<AdaptiveTableRecord> tableRecords = new LinkedList<>();

        for ( CatalogTable catalogTable : catalogTables ) {
            List<String> columns = catalogTable.fieldIds.stream().map( catalog::getColumn ).map( c -> catalogTable.name + "." + c.name ).collect( Collectors.toList());
            tableRecords.add( new AdaptiveTableRecord( catalogTable.name, columns ) );
        }

        return tableRecords;
    }


    public static Set<Pair<String, String>> getForeignKeyReferences( Catalog catalog, List<CatalogTable> catalogTables ) {
        // Parse tables again for all foreign key references...
        Set<Pair<String, String>> references = new HashSet<>();

        for ( CatalogTable catalogTableA : catalogTables ) {
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

        return references;
    }


    public static Set<Pair<String, String>> getPolyTypePartners( Catalog catalog, List<CatalogTable> catalogTables ) {

        // Parse tables again for all tuples of matching PolyTypes...
        Set<Pair<String, String>> polyTypePartners = new HashSet<>();

        for ( CatalogTable catalogTableA : catalogTables ) {
            List<CatalogColumn> catalogColumnsA = catalog.getColumns( catalogTableA.id );
            for ( CatalogTable catalogTableB : catalogTables ) {
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

        return polyTypePartners;
    }


}
