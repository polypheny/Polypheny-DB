/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.type;


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;


/**
 * SQL map type.
 */
public class MapPolyType extends AbstractPolyType {

    private final AlgDataType keyType;
    private final AlgDataType valueType;


    /**
     * Creates a MapSqlType. This constructor should only be called from a factory method.
     */
    public MapPolyType( AlgDataType keyType, AlgDataType valueType, boolean isNullable ) {
        super( PolyType.MAP, isNullable, null );
        assert keyType != null;
        assert valueType != null;
        this.keyType = keyType;
        this.valueType = valueType;
        computeDigest();
    }


    @Override
    public AlgDataType getValueType() {
        return valueType;
    }


    @Override
    public AlgDataType getKeyType() {
        return keyType;
    }


    // implement RelDataTypeImpl
    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        sb.append( "(" )
                .append( withDetail ? keyType.getFullTypeString() : keyType.toString() )
                .append( ", " )
                .append( withDetail ? valueType.getFullTypeString() : valueType.toString() )
                .append( ") MAP" );
    }


    // implement RelDataType
    @Override
    public AlgDataTypeFamily getFamily() {
        return this;
    }

}

