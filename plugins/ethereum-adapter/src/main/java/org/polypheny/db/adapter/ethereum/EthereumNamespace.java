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

package org.polypheny.db.adapter.ethereum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.ethereum.EthereumPlugin.EthereumDataSource;
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

public class EthereumNamespace extends Namespace {

    private final String clientUrl;
    private final Map<String, EthereumTable> tableMap = new HashMap<>();


    public EthereumNamespace( long id, long adapterId, String clientUrl ) {
        super( id, adapterId );
        this.clientUrl = clientUrl;
    }


    public EthereumTable createBlockchainTable( PhysicalTable table, EthereumDataSource ethereumDataSource ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<EthereumFieldType> fieldTypes = new LinkedList<>();
        List<Integer> fieldIds = new ArrayList<>( table.columns.size() );
        for ( PhysicalColumn column : table.columns ) {
            AlgDataType sqlType = sqlType( typeFactory, column.type, column.length, column.scale, null );
            fieldInfo.add( column.id, column.name, column.name, sqlType ).nullable( column.nullable );
            fieldTypes.add( EthereumFieldType.getBlockchainFieldType( column.type ) );
            fieldIds.add( column.position );
        }

        int[] fields = fieldIds.stream().mapToInt( i -> i ).toArray();
        EthereumMapper mapper = table.name.equals( "block" ) ? EthereumMapper.BLOCK : EthereumMapper.TRANSACTION;
        EthereumTable physical = new EthereumTable( table, clientUrl, AlgDataTypeImpl.proto( fieldInfo.build() ), fieldTypes, fields, mapper, ethereumDataSource );
        tableMap.put( physical.name, physical );
        return physical;
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
