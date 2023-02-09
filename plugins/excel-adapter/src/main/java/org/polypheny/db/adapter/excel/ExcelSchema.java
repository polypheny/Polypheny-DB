/*
 * Copyright 2019-2023 The Polypheny Project
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

public class ExcelSchema extends AbstractSchema {

    private final URL directoryUrl;
    private final ExcelTable.Flavor flavor;
    private final Map<String, ExcelTable> tableMap = new HashMap<>();
    private final String sheet;


    /**
     * Creates an Excel schema.
     *
     * @param directoryUrl Directory that holds {@code .Excel} files
     */
    public ExcelSchema( URL directoryUrl, ExcelTable.Flavor flavor ) {
        super();
        this.directoryUrl = directoryUrl;
        this.flavor = flavor;
        this.sheet = "";
    }


    public ExcelSchema( URL directoryUrl, ExcelTable.Flavor flavor, String sheet ) {
        super();
        this.directoryUrl = directoryUrl;
        this.flavor = flavor;
        this.sheet = sheet;
    }


    public Table createExcelTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, ExcelSource excelSource, CatalogPartitionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<ExcelFieldType> fieldTypes = new LinkedList<>();
        List<Integer> fieldIds = new ArrayList<>( columnPlacementsOnStore.size() );
        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            AlgDataType sqlType = sqlType( typeFactory, catalogColumn.type, catalogColumn.length, catalogColumn.scale, null );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
            fieldTypes.add( ExcelFieldType.getExcelFieldType( catalogColumn.type ) );
            fieldIds.add( (int) placement.physicalPosition );
        }

        String excelFileName = Catalog
                .getInstance()
                .getColumnPlacementsOnAdapterPerTable( excelSource.getAdapterId(), catalogTable.id ).iterator().next()
                .physicalSchemaName;

        Source source;
        try {
            source = Sources.of( new URL( directoryUrl, excelFileName ) );
        } catch ( MalformedURLException e ) {
            throw new RuntimeException( e );
        }
        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();
        ExcelTable table = createTable( source, AlgDataTypeImpl.proto( fieldInfo.build() ), fieldTypes, fields, excelSource, catalogTable.id );
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
    private ExcelTable createTable( Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource, Long tableId ) {
        if ( this.sheet.equals( "" ) ) {
            switch ( flavor ) {
                case TRANSLATABLE:
                    return new ExcelTranslatableTable( source, protoRowType, fieldTypes, fields, excelSource, tableId );
                case SCANNABLE:
                    return new ExcelScannableTable( source, protoRowType, fieldTypes, fields, excelSource, tableId );
                case FILTERABLE:
                    return new ExcelFilterableTable( source, protoRowType, fieldTypes, fields, excelSource, tableId );
                default:
                    throw new AssertionError( "Unknown flavor " + this.flavor );
            }
        } else {
            switch ( flavor ) {
                case TRANSLATABLE:
                    return new ExcelTranslatableTable( source, protoRowType, fieldTypes, fields, excelSource, tableId, this.sheet );
                case SCANNABLE:
                    return new ExcelScannableTable( source, protoRowType, fieldTypes, fields, excelSource, tableId, this.sheet );
                case FILTERABLE:
                    return new ExcelFilterableTable( source, protoRowType, fieldTypes, fields, excelSource, tableId, this.sheet );
                default:
                    throw new AssertionError( "Unknown flavor " + this.flavor );
            }
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

}
