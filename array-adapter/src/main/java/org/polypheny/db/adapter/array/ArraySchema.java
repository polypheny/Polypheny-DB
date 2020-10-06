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

package org.polypheny.db.adapter.array;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.sql2rel.NullInitializerExpressionFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Util;


public class ArraySchema extends AbstractSchema {

    private final Map<String, ArrayTable> tableMap = new HashMap<>();


    /**
     * Creates a Array schema.
     */
    public ArraySchema() {
        super();
    }


    public Table createTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, ArrayStore arrayStore ) {
        final RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            RelDataType sqlType = sqlType( typeFactory, catalogColumn.type, catalogColumn.length, catalogColumn.scale, null );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
        }

        String physicalTableName = Catalog.getInstance().getColumnPlacementsOnStore( arrayStore.getStoreId(), catalogTable.id ).iterator().next().physicalTableName;
        ArrayTable table = new ArrayTable( physicalTableName, RelDataTypeImpl.proto( fieldInfo.build() ), new NullInitializerExpressionFactory(), arrayStore );
        tableMap.put( catalogTable.name, table );
        return table;
    }


    @Override
    public Map<String, Table> getTableMap() {
        return new HashMap<>( tableMap );
    }


    private RelDataType sqlType( RelDataTypeFactory typeFactory, PolyType dataTypeName, Integer length, Integer scale, String typeString ) {
        // Fall back to ANY if type is unknown
        final PolyType polyType = Util.first( dataTypeName, PolyType.ANY );
        switch ( polyType ) {
            case ARRAY:
                RelDataType component = null;
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

