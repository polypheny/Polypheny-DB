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

package ch.unibas.dmi.dbis.polyphenydb.rel.type;


import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeExplicitPrecedenceList;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Implementation of {@link RelDataType} for a dynamic table.
 *
 * It's used during SQL validation, where the field list is mutable for the getField() call. After SQL validation, a normal {@link RelDataTypeImpl} with an immutable field list takes the place
 * of the DynamicRecordTypeImpl instance.
 */
public class DynamicRecordTypeImpl extends DynamicRecordType {

    private final RelDataTypeHolder holder;


    /**
     * Creates a DynamicRecordTypeImpl.
     */
    public DynamicRecordTypeImpl( RelDataTypeFactory typeFactory ) {
        this.holder = new RelDataTypeHolder( typeFactory );
        computeDigest();
    }


    @Override
    public List<RelDataTypeField> getFieldList() {
        return holder.getFieldList();
    }


    @Override
    public int getFieldCount() {
        return holder.getFieldCount();
    }


    @Override
    public RelDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        final Pair<RelDataTypeField, Boolean> pair = holder.getFieldOrInsert( fieldName, caseSensitive );
        // If a new field is added, we should re-compute the digest.
        if ( pair.right ) {
            computeDigest();
        }

        return pair.left;
    }


    @Override
    public List<String> getFieldNames() {
        return holder.getFieldNames();
    }


    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ROW;
    }


    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new SqlTypeExplicitPrecedenceList( ImmutableList.of() );
    }


    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        sb.append( "(DynamicRecordRow" ).append( getFieldNames() ).append( ")" );
    }


    @Override
    public boolean isStruct() {
        return true;
    }


    @Override
    public RelDataTypeFamily getFamily() {
        return getSqlTypeName().getFamily();
    }

}

