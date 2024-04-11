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
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.Util;


/**
 * Schema mapped onto a directory of CSV files. Each table in the schema is a CSV file in that directory.
 */
public class CsvSchema extends Namespace {

    private final URL directoryUrl;
    private final CsvTable.Flavor flavor;
    @Getter
    private final Map<String, CsvTable> tableMap = new HashMap<>();


    /**
     * Creates a CSV schema.
     *
     * @param directoryUrl Directory that holds {@code .csv} files
     * @param flavor Whether to instantiate flavor tables that undergo query optimization
     */
    public CsvSchema( long id, long adapterId, URL directoryUrl, CsvTable.Flavor flavor ) {
        super( id, adapterId );
        this.directoryUrl = directoryUrl;
        this.flavor = flavor;
    }


    public CsvTable createCsvTable( long id, PhysicalTable table, CsvSource csvSource ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final Builder fieldInfo = typeFactory.builder();
        List<CsvFieldType> fieldTypes = new LinkedList<>();
        List<Integer> fieldIds = new ArrayList<>();

        List<ExportedColumn> columns = csvSource.getExportedColumns().get( table.name );

        for ( PhysicalColumn column : table.getColumns() ) {
            AlgDataType sqlType = sqlType( typeFactory, column.type, column.length, column.scale, null );
            fieldInfo.add( column.id, column.name, columns.get( column.position ).physicalColumnName, sqlType ).nullable( column.nullable );
            fieldTypes.add( CsvFieldType.getCsvFieldType( column.type ) );
            fieldIds.add( column.position );
        }

        String csvFileName = columns.get( 0 ).physicalSchemaName;
        Source source;
        try {
            source = Sources.of( new URL( directoryUrl, csvFileName ) );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();

        return createTable( id, source, table, fieldTypes, fields, csvSource );
    }


    /**
     * Creates different subtype of table based on the "flavor" attribute.
     */
    private CsvTable createTable( long id, Source source, PhysicalTable table, List<CsvFieldType> fieldTypes, int[] fields, CsvSource csvSource ) {
        return switch ( flavor ) {
            case TRANSLATABLE -> new CsvTranslatableTable( id, source, table, fieldTypes, fields, csvSource );
            case SCANNABLE -> new CsvScannableTable( id, source, table, fieldTypes, fields, csvSource );
            case FILTERABLE -> new CsvFilterableTable( id, source, table, fieldTypes, fields, csvSource );
        };
    }


    private AlgDataType sqlType( AlgDataTypeFactory typeFactory, PolyType dataTypeName, Integer length, Integer scale, String typeString ) {
        // Fall back to ANY if type is unknown
        final PolyType polyType = Util.first( dataTypeName, PolyType.ANY );
        if ( polyType == PolyType.ARRAY ) {
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


    @Override
    protected @Nullable Convention getConvention() {
        return null; // No convention
    }

}

