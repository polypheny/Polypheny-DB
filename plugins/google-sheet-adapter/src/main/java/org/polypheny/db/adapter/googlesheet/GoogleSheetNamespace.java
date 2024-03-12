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

package org.polypheny.db.adapter.googlesheet;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Util;


/**
 * Schema mapped onto a Google Sheet URL. Each table in the schema is a sheet in the URL.
 */
public class GoogleSheetNamespace extends Namespace {

    private final URL sheetsURL;
    private final int querySize;
    @Getter
    private final GoogleSheetSource source;
    private Map<String, GoogleSheetTable> tableMap = new HashMap<>();


    /**
     * Creates a GoogleSheet Schema
     *
     * @param sheetsURL - the url of the Google Sheet
     * @param querySize - the size of each query while scanning
     */
    public GoogleSheetNamespace( long id, long adapterId, URL sheetsURL, int querySize, GoogleSheetSource source ) {
        super( id, adapterId );
        this.sheetsURL = sheetsURL;
        this.querySize = querySize;
        this.source = source;
    }


    public PhysicalTable createGoogleSheetTable( PhysicalTable table, GoogleSheetSource googleSheetSource ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<GoogleSheetFieldType> fieldTypes = new LinkedList<>();
        List<Integer> fieldIds = new ArrayList<>( table.columns.size() );

        for ( PhysicalColumn column : table.columns ) {
            AlgDataType sqlType = sqlType( typeFactory, column.type, column.length, column.scale, null );
            fieldInfo.add( column.id, column.name, column.name, sqlType ).nullable( column.nullable );
            fieldTypes.add( GoogleSheetFieldType.getGoogleSheetFieldType( column.type ) );
            fieldIds.add( column.position );
        }

        String tableName = googleSheetSource.sheet;

        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();
        // build table and return later based on what you need for the table
        GoogleSheetTable physical = new GoogleSheetTable( table, sheetsURL, querySize, tableName, AlgDataTypeImpl.proto( fieldInfo.build() ), fields, googleSheetSource, fieldTypes );
        tableMap.put( physical.name, physical );
        return table;
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
