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

package org.polypheny.db.type;


import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFamily;
import org.polypheny.db.rel.type.RelDataTypePrecedenceList;


/**
 * Array type.
 */
@Accessors(chain = true)
public class ArrayType extends AbstractPolyType {

    private final RelDataType elementType;
    @Getter
    @Setter
    private long cardinality;
    @Getter
    @Setter
    private long dimension;


    /**
     * Creates an ArrayType. This constructor should only be called from a factory method.
     */
    public ArrayType( RelDataType elementType, boolean isNullable ) {
        this( elementType, isNullable, -1, -1 );
    }


    /**
     * Creates an ArrayType. This constructor should only be called from a factory method.
     */
    public ArrayType( final RelDataType elementType, final boolean isNullable, final long cardinality, final long dimension ) {
        super( PolyType.ARRAY, isNullable, null );
        this.elementType = Objects.requireNonNull( elementType );
        computeDigest();
        this.cardinality = cardinality;
        this.dimension = dimension;
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
        if( withDetail ) {
            sb.append( String.format( "(%d,%d)", cardinality, dimension ) );
        }
    }


    // implement RelDataType
    @Override
    public RelDataType getComponentType() {
        return elementType;
    }


    /*
    * @return This returns the type of a nested ArrayType. E.g. for an array of an array of Integers, this will return the Integer type.
    */
    public RelDataType getNestedComponentType() {
        if( this.getComponentType().getPolyType() == PolyType.ARRAY ) {
            return ((ArrayType) this.elementType).getNestedComponentType();
        } else {
            return this.elementType;
        }
    }


    /**
     * @return the largest cardinality of all nested arrays
     */
    public long getMaxCardinality () {
        if( this.elementType != null && this.elementType instanceof ArrayType ) {
            return Math.max( this.cardinality, ((ArrayType) this.elementType).getMaxCardinality() );
        }
        else {
            return this.cardinality;
        }
    }

    /**
     * Return the largest dimension of all nested arrays
     * E.g. if array b is within array a (a = [b1,b2]), then the array b has dimension 2 (it sits in the second dimension) and array a has dimension 1
     * It might make sense to turn it around, but with this approach, the outermost SqlArrayValueConstructor has dimension 1 and knows that it is the outermost one, which is important for the unparsing
     */
    public long getMaxDimension () {
        if( this.elementType != null && this.elementType instanceof ArrayType ) {
            return Math.max( this.dimension, ((ArrayType) this.elementType).getMaxDimension() );
        }
        else {
            return this.dimension;
        }
    }


    // implement RelDataType
    @Override
    public RelDataTypeFamily getFamily() {
        return this;
    }


    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new RelDataTypePrecedenceList() {
            @Override
            public boolean containsType( RelDataType type ) {
                return type.getPolyType() == getPolyType()
                        && type.getComponentType() != null
                        && getComponentType().getPrecedenceList().containsType( type.getComponentType() );
            }


            @Override
            public int compareTypePrecedence( RelDataType type1, RelDataType type2 ) {
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
