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

package org.polypheny.db.adapter.csv;


import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.Util;


/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {

    private final URL directoryUrl;
    private final CsvTable.Flavor flavor;
    private Map<String, CsvTable> tableMap = new HashMap<>();


    /**
     * Creates a CSV schema.
     *
     * @param directoryUrl Directory that holds {@code .csv} files
     * @param flavor Whether to instantiate flavor tables that undergo query optimization
     */
    public CsvSchema( URL directoryUrl, CsvTable.Flavor flavor ) {
        super();
        this.directoryUrl = directoryUrl;
        this.flavor = flavor;
    }


    public Table createCsvTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CsvSource csvSource, CatalogPartitionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<CsvFieldType> fieldTypes = new LinkedList<>();
        List<Integer> fieldIds = new ArrayList<>( columnPlacementsOnStore.size() );
        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            AlgDataType sqlType = sqlType( typeFactory, catalogColumn.type, catalogColumn.length, catalogColumn.scale, null );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
            fieldTypes.add( CsvFieldType.getCsvFieldType( catalogColumn.type ) );
            fieldIds.add( (int) placement.physicalPosition );
        }

        String csvFileName = Catalog
                .getInstance()
                .getColumnPlacementsOnAdapterPerTable( csvSource.getAdapterId(), catalogTable.id ).iterator().next()
                .physicalSchemaName;
        Source source;
        try {
            source = Sources.of( new URL( directoryUrl, csvFileName ) );
        } catch ( MalformedURLException e ) {
            throw new RuntimeException( e );
        }
        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();
        CsvTable table = createTable( source, AlgDataTypeImpl.proto( fieldInfo.build() ), fieldTypes, fields, csvSource, catalogTable.id );
        tableMap.put( catalogTable.name + "_" + partitionPlacement.partitionId, table );
        return table;
    }


    @Override
    public Map<String, Table> getTableMap() {
        return new HashMap<>( tableMap );
    }


    /**
     * Creates different sub-type of table based on the "flavor" attribute.
     */
    private CsvTable createTable( Source source, AlgProtoDataType protoRowType, List<CsvFieldType> fieldTypes, int[] fields, CsvSource csvSource, Long tableId ) {
        switch ( flavor ) {
            case TRANSLATABLE:
                return new CsvTranslatableTable( source, protoRowType, fieldTypes, fields, csvSource, tableId );
            case SCANNABLE:
                return new CsvScannableTable( source, protoRowType, fieldTypes, fields, csvSource, tableId );
            case FILTERABLE:
                return new CsvFilterableTable( source, protoRowType, fieldTypes, fields, csvSource, tableId );
            default:
                throw new AssertionError( "Unknown flavor " + this.flavor );
        }
    }


    private AlgDataType sqlType( AlgDataTypeFactory typeFactory, PolyType dataTypeName, Integer length, Integer scale, String typeString ) {
        // Fall back to ANY if type is unknown
        final PolyType polyType = Util.first( dataTypeName, PolyType.ANY );
        switch ( polyType ) {
            case ARRAY:
                AlgDataType component = null;
                if ( typeString != null && typeString.endsWith( " ARRAY" ) ) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type "INTEGER".
                    final String remaining = typeString.substring( 0, typeString.length() - " ARRAY".length() );
                    component = parseTypeString( typeFactory, remaining );
                }
                if ( component == null ) {
                    component = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
                }
                return typeFactory.createArrayType( component, -1 );
        }
        if ( scale != null && length != null && length >= 0 && scale >= 0 && polyType.allowsPrecScale( true, true ) ) {
            return typeFactory.createPolyType( polyType, length, scale );
        } else if ( length != null && length >= 0 && polyType.allowsPrecNoScale() ) {
            return typeFactory.createPolyType( polyType, length );
        } else {
            assert polyType.allowsNoPrecNoScale();
            return typeFactory.createPolyType( polyType );
        }
    }


    /**
     * Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).
     */
    private AlgDataType parseTypeString( AlgDataTypeFactory typeFactory, String typeString ) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf( "(" );
        if ( open >= 0 ) {
            int close = typeString.indexOf( ")", open );
            if ( close >= 0 ) {
                String rest = typeString.substring( open + 1, close );
                typeString = typeString.substring( 0, open );
                int comma = rest.indexOf( "," );
                if ( comma >= 0 ) {
                    precision = Integer.parseInt( rest.substring( 0, comma ) );
                    scale = Integer.parseInt( rest.substring( comma ) );
                } else {
                    precision = Integer.parseInt( rest );
                }
            }
        }
        try {
            final PolyType typeName = PolyType.valueOf( typeString );
            return typeName.allowsPrecScale( true, true )
                    ? typeFactory.createPolyType( typeName, precision, scale )
                    : typeName.allowsPrecScale( true, false )
                            ? typeFactory.createPolyType( typeName, precision )
                            : typeFactory.createPolyType( typeName );
        } catch ( IllegalArgumentException e ) {
            return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
        }
    }

//    /**
//     * Looks for a suffix on a string and returns either the string with the suffix removed or the original string.
//     */
//    private static String trim( String s, String suffix ) {
//        String trimmed = trimOrNull( s, suffix );
//        return trimmed != null ? trimmed : s;
//    }
//
//
//    /**
//     * Looks for a suffix on a string and returns either the string with the suffix removed or null.
//     */
//    private static String trimOrNull( String s, String suffix ) {
//        return s.endsWith( suffix )
//                ? s.substring( 0, s.length() - suffix.length() )
//                : null;
//    }
//
//
//    @Override
//    public Map<String, Table> getTableMap() {
//        if ( tableMap == null ) {
//            tableMap = createTableMap();
//        }
//        return tableMap;
//    }
//
//
//    private Map<String, Table> createTableMap() {
//        // Look for files in the directory ending in ".csv", ".csv.gz", ".json", ".json.gz".
//        final Source baseSource = Sources.of( directoryFile );
//        File[] files = directoryFile.listFiles( ( dir, name ) -> {
//            final String nameSansGz = trim( name, ".gz" );
//            return nameSansGz.endsWith( ".csv" ) || nameSansGz.endsWith( ".json" );
//        } );
//        if ( files == null ) {
//            System.out.println( "directory " + directoryFile + " not found" );
//            files = new File[0];
//        }
//        // Build a map from table name to table; each file becomes a table.
//        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
//        for ( File file : files ) {
//            Source source = Sources.of( file );
//            Source sourceSansGz = source.trim( ".gz" );
//            final Source sourceSansJson = sourceSansGz.trimOrNull( ".json" );
//            if ( sourceSansJson != null ) {
//                JsonTable table = new JsonTable( source );
//                builder.put( sourceSansJson.relative( baseSource ).path(), table );
//                continue;
//            }
//            final Source sourceSansCsv = sourceSansGz.trim( ".csv" );
//
//            final Table table = createTable( source );
//            builder.put( sourceSansCsv.relative( baseSource ).path(), table );
//        }
//        return builder.build();
//    }
//
//
//

}

