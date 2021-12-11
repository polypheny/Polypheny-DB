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


import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;
import org.polypheny.db.algebra.type.AlgDataTypePrecedenceList;


/**
 * Array type.
 */
@Accessors(chain = true)
public class ArrayType extends AbstractPolyType {

    private final AlgDataType elementType;
    @Getter
    @Setter
    private long cardinality;
    @Getter
    @Setter
    private long dimension;


    /**
     * Creates an ArrayType. This constructor should only be called from a factory method.
     */
    public ArrayType( AlgDataType elementType, boolean isNullable ) {
        this( elementType, isNullable, -1, -1 );
    }


    /**
     * Creates an ArrayType. This constructor should only be called from a factory method.
     */
    public ArrayType( final AlgDataType elementType, final boolean isNullable, final long cardinality, final long dimension ) {
        super( PolyType.ARRAY, isNullable, null );
        this.elementType = Objects.requireNonNull( elementType );
        this.cardinality = cardinality;
        this.dimension = dimension;
        computeDigest();
    }


    // implement RelDataTypeImpl
    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        if ( withDetail ) {
            sb.append( elementType.getFullTypeString() );
        } else {
            sb.append( elementType.toString() );
        }
        sb.append( " ARRAY" );
        if ( withDetail ) {
            sb.append( String.format( "(%d,%d)", cardinality, dimension ) );
        }
    }


    // implement RelDataType
    @Override
    public AlgDataType getComponentType() {
        return elementType;
    }


    /*
     * @return This returns the type of a nested ArrayType. E.g. for an array of an array of Integers, this will return the Integer type.
     */
    public AlgDataType getNestedComponentType() {
        if ( this.getComponentType().getPolyType() == PolyType.ARRAY ) {
            return ((ArrayType) this.elementType).getNestedComponentType();
        } else {
            return this.elementType;
        }
    }


    /**
     * @return the largest cardinality of all nested arrays
     */
    public long getMaxCardinality() {
        return this.cardinality;
    }


    /**
     * @return the dimension of this array
     */
    public long getMaxDimension() {
        return this.dimension;
    }


    // implement RelDataType
    @Override
    public AlgDataTypeFamily getFamily() {
        return this;
    }


    @Override
    public AlgDataTypePrecedenceList getPrecedenceList() {
        return new AlgDataTypePrecedenceList() {
            @Override
            public boolean containsType( AlgDataType type ) {
                return type.getPolyType() == getPolyType()
                        && type.getComponentType() != null
                        && getComponentType().getPrecedenceList().containsType( type.getComponentType() );
            }


            @Override
            public int compareTypePrecedence( AlgDataType type1, AlgDataType type2 ) {
                if ( !containsType( type1 ) ) {
                    throw new IllegalArgumentException( "must contain type: " + type1 );
                }
                if ( !containsType( type2 ) ) {
                    throw new IllegalArgumentException( "must contain type: " + type2 );
                }
                return getComponentType().getPrecedenceList().compareTypePrecedence( type1.getComponentType(), type2.getComponentType() );
            }
        };
    }

}
