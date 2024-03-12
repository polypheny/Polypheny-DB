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

package org.polypheny.db.adapter.excel;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
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

public class ExcelNamespace extends Namespace {

    private final URL directoryUrl;
    private final ExcelTable.Flavor flavor;
    private final Map<String, ExcelTable> tableMap = new HashMap<>();
    private final String sheet;


    /**
     * Creates an Excel schema.
     *
     * @param directoryUrl Directory that holds {@code .Excel} files
     */
    public ExcelNamespace( long id, long adapterId, URL directoryUrl, ExcelTable.Flavor flavor ) {
        this( id, adapterId, directoryUrl, flavor, "" );
    }


    public ExcelNamespace( long id, long adapterId, URL directoryUrl, ExcelTable.Flavor flavor, String sheet ) {
        super( id, adapterId );
        this.directoryUrl = directoryUrl;
        this.flavor = flavor;
        this.sheet = sheet;
    }


    public ExcelTable createExcelTable( PhysicalTable table, ExcelSource excelSource ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<ExcelFieldType> fieldTypes = new LinkedList<>();
        List<Integer> fieldIds = new ArrayList<>( table.columns.size() );
        for ( PhysicalColumn column : table.columns ) {
            AlgDataType sqlType = sqlType( typeFactory, column.type, column.length, column.scale, null );
            fieldInfo.add( column.id, column.name, column.name, sqlType ).nullable( column.nullable );
            fieldTypes.add( ExcelFieldType.getExcelFieldType( column.type ) );
            fieldIds.add( column.position );
        }

        String excelFileName = excelSource.sheetName;

        Source source;
        try {
            source = Sources.of( new URL( directoryUrl, excelFileName ) );
        } catch ( MalformedURLException e ) {
            throw new GenericRuntimeException( e );
        }
        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();
        ExcelTable physical = createTable( table, source, AlgDataTypeImpl.proto( fieldInfo.build() ), fieldTypes, fields, excelSource );
        tableMap.put( physical.name + "_" + physical.allocationId, physical );
        return physical;

    }


    /**
     * Creates different sub-type of table based on the "flavor" attribute.
     */
    private ExcelTable createTable( PhysicalTable table, Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource ) {
        if ( this.sheet.isEmpty() ) {
            return switch ( flavor ) {
                case TRANSLATABLE -> new ExcelTranslatableTable( table, source, protoRowType, fieldTypes, fields, excelSource );
                case SCANNABLE -> new ExcelScannableTable( table, source, protoRowType, fieldTypes, fields, excelSource );
                case FILTERABLE -> new ExcelFilterableTable( table, source, protoRowType, fieldTypes, fields, excelSource );
            };
        } else {
            return switch ( flavor ) {
                case TRANSLATABLE -> new ExcelTranslatableTable( table, source, protoRowType, fieldTypes, fields, excelSource, this.sheet );
                case SCANNABLE -> new ExcelScannableTable( table, source, protoRowType, fieldTypes, fields, excelSource, this.sheet );
                case FILTERABLE -> new ExcelFilterableTable( table, source, protoRowType, fieldTypes, fields, excelSource, this.sheet );
            };
        }

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
        return null; // no convention
    }

}
