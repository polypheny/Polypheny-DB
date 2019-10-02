/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypePrecedenceList;


/**
 * MultisetSqlType represents a standard SQL2003 multiset type.
 */
public class MultisetSqlType extends AbstractSqlType {

    private final RelDataType elementType;


    /**
     * Constructs a new MultisetSqlType. This constructor should only be called from a factory method.
     */
    public MultisetSqlType( RelDataType elementType, boolean isNullable ) {
        super( SqlTypeName.MULTISET, isNullable, null );
        assert elementType != null;
        this.elementType = elementType;
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
        sb.append( " MULTISET" );
    }


    // implement RelDataType
    @Override
    public RelDataType getComponentType() {
        return elementType;
    }


    // implement RelDataType
    @Override
    public RelDataTypeFamily getFamily() {
        // TODO jvs 2-Dec-2004:  This gives each multiset type its own family.  But that's not quite correct; the family should be based on the element type for proper comparability semantics (per 4.10.4 of SQL/2003).
        //  So either this should make up canonical families dynamically, or the comparison type-checking should not rely on this.  I think the same goes for ROW types.
        return this;
    }


    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new RelDataTypePrecedenceList() {
            @Override
            public boolean containsType( RelDataType type ) {
                return type.getSqlTypeName() == getSqlTypeName()
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
                return getComponentType()
                        .getPrecedenceList()
                        .compareTypePrecedence( type1.getComponentType(), type2.getComponentType() );
            }
        };
    }
}

