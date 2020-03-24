/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.CatalogManager;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.sql.type.SqlTypeFactoryImpl;
import org.polypheny.db.sql.type.SqlTypeName;
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


    public Table createCsvTable( CatalogTable catalogTable, CsvStore csvStore ) {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<CsvFieldType> fieldTypes = new LinkedList<>();
        for ( CatalogColumnPlacement placement : CatalogManager.getInstance().getCatalog().getColumnPlacementsOnStore( csvStore.getStoreId(), catalogTable.id ) ) {
            CatalogColumn catalogColumn = null;
            // TODO MV: This is not efficient
            // Get catalog column

            try {
                catalogColumn = CatalogManager.getInstance().getCatalog().getColumn( placement.columnId );
            } catch ( UnknownColumnException | GenericCatalogException e ) {
                throw new RuntimeException( "Column not found." ); // This should not happen
            }

            SqlTypeName dataTypeName = SqlTypeName.get( catalogColumn.type.name() ); // TODO Replace PolySqlType with native
            RelDataType sqlType = sqlType( typeFactory, dataTypeName, catalogColumn.length, catalogColumn.scale, null );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
            fieldTypes.add( CsvFieldType.getCsvFieldType( catalogColumn.type ) );
        }

        // TODO MV: This assumes that all physical columns of a logical table are in the same csv file
        String csvFileName = CatalogManager.getInstance().getCatalog().getColumnPlacementsOnStore( csvStore.getStoreId() ).iterator().next().physicalTableName + ".csv";
        Source source;
        try {
            source = Sources.of( new URL( directoryUrl, csvFileName ) );
        } catch ( MalformedURLException e ) {
            throw new RuntimeException( e );
        }
        CsvTable table = createTable( source, RelDataTypeImpl.proto( fieldInfo.build() ), fieldTypes, csvStore );
        tableMap.put( catalogTable.name, table );
        return table;
    }


    @Override
    public Map<String, Table> getTableMap() {
        return new HashMap<>( tableMap );
    }


    /**
     * Creates different sub-type of table based on the "flavor" attribute.
     */
    private CsvTable createTable( Source source, RelProtoDataType protoRowType, List<CsvFieldType> fieldTypes, CsvStore csvStore ) {
        switch ( flavor ) {
            case TRANSLATABLE:
                return new CsvTranslatableTable( source, protoRowType, fieldTypes, csvStore );
            case SCANNABLE:
                return new CsvScannableTable( source, protoRowType, fieldTypes, csvStore );
            case FILTERABLE:
                return new CsvFilterableTable( source, protoRowType, fieldTypes, csvStore );
            default:
                throw new AssertionError( "Unknown flavor " + this.flavor );
        }
    }


    private RelDataType sqlType( RelDataTypeFactory typeFactory, SqlTypeName dataTypeName, Integer length, Integer scale, String typeString ) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName = Util.first( dataTypeName, SqlTypeName.ANY );
        switch ( sqlTypeName ) {
            case ARRAY:
                RelDataType component = null;
                if ( typeString != null && typeString.endsWith( " ARRAY" ) ) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type "INTEGER".
                    final String remaining = typeString.substring( 0, typeString.length() - " ARRAY".length() );
                    component = parseTypeString( typeFactory, remaining );
                }
                if ( component == null ) {
                    component = typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
                }
                return typeFactory.createArrayType( component, -1 );
        }
        if ( scale != null && length != null && length >= 0 && scale >= 0 && sqlTypeName.allowsPrecScale( true, true ) ) {
            return typeFactory.createSqlType( sqlTypeName, length, scale );
        } else if ( length != null && length >= 0 && sqlTypeName.allowsPrecNoScale() ) {
            return typeFactory.createSqlType( sqlTypeName, length );
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType( sqlTypeName );
        }
    }


    /**
     * Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).
     */
    private RelDataType parseTypeString( RelDataTypeFactory typeFactory, String typeString ) {
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
            final SqlTypeName typeName = SqlTypeName.valueOf( typeString );
            return typeName.allowsPrecScale( true, true )
                    ? typeFactory.createSqlType( typeName, precision, scale )
                    : typeName.allowsPrecScale( true, false )
                            ? typeFactory.createSqlType( typeName, precision )
                            : typeFactory.createSqlType( typeName );
        } catch ( IllegalArgumentException e ) {
            return typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
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

