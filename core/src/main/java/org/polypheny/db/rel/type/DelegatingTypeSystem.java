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

package org.polypheny.db.rel.type;


import org.polypheny.db.type.PolyType;


/**
 * Implementation of {@link org.polypheny.db.rel.type.RelDataTypeSystem} that sends all methods to an underlying object.
 */
public class DelegatingTypeSystem implements RelDataTypeSystem {

    private final RelDataTypeSystem typeSystem;


    /**
     * Creates a {@code DelegatingTypeSystem}.
     */
    protected DelegatingTypeSystem( RelDataTypeSystem typeSystem ) {
        this.typeSystem = typeSystem;
    }


    @Override
    public int getMaxScale( PolyType typeName ) {
        return typeSystem.getMaxScale( typeName );
    }


    @Override
    public int getDefaultPrecision( PolyType typeName ) {
        return typeSystem.getDefaultPrecision( typeName );
    }


    @Override
    public int getMaxPrecision( PolyType typeName ) {
        return typeSystem.getMaxPrecision( typeName );
    }


    @Override
    public int getMaxNumericScale() {
        return typeSystem.getMaxNumericScale();
    }


    @Override
    public int getMaxNumericPrecision() {
        return typeSystem.getMaxNumericPrecision();
    }


    @Override
    public String getLiteral( PolyType typeName, boolean isPrefix ) {
        return typeSystem.getLiteral( typeName, isPrefix );
    }


    @Override
    public boolean isCaseSensitive( PolyType typeName ) {
        return typeSystem.isCaseSensitive( typeName );
    }


    @Override
    public boolean isAutoincrement( PolyType typeName ) {
        return typeSystem.isAutoincrement( typeName );
    }


    @Override
    public int getNumTypeRadix( PolyType typeName ) {
        return typeSystem.getNumTypeRadix( typeName );
    }


    @Override
    public RelDataType deriveSumType( RelDataTypeFactory typeFactory, RelDataType argumentType ) {
        return typeSystem.deriveSumType( typeFactory, argumentType );
    }


    @Override
    public RelDataType deriveAvgAggType( RelDataTypeFactory typeFactory, RelDataType argumentType ) {
        return typeSystem.deriveAvgAggType( typeFactory, argumentType );
    }


    @Override
    public RelDataType deriveCovarType( RelDataTypeFactory typeFactory, RelDataType arg0Type, RelDataType arg1Type ) {
        return typeSystem.deriveCovarType( typeFactory, arg0Type, arg1Type );
    }


    @Override
    public RelDataType deriveFractionalRankType( RelDataTypeFactory typeFactory ) {
        return typeSystem.deriveFractionalRankType( typeFactory );
    }


    @Override
    public RelDataType deriveRankType( RelDataTypeFactory typeFactory ) {
        return typeSystem.deriveRankType( typeFactory );
    }


    @Override
    public boolean isSchemaCaseSensitive() {
        return typeSystem.isSchemaCaseSensitive();
    }


    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return typeSystem.shouldConvertRaggedUnionTypesToVarying();
    }
}

