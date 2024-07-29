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

package org.polypheny.db.algebra.type;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeExplicitPrecedenceList;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link AlgDataType} for a dynamic table.
 * <p>
 * It's used during SQL validation, where the field list is mutable for the getField() call. After SQL validation, a
 * normal {@link AlgDataTypeImpl} with an immutable field list takes the place of the DynamicRecordTypeImpl instance.
 */
public class DynamicRecordTypeImpl extends DynamicRecordType {

    private final AlgDataTypeHolder holder;


    /**
     * Creates a DynamicRecordTypeImpl.
     */
    public DynamicRecordTypeImpl( AlgDataTypeFactory typeFactory ) {
        this.holder = new AlgDataTypeHolder( typeFactory );
        computeDigest();
    }


    @Override
    public List<AlgDataTypeField> getFields() {
        return holder.getFieldList();
    }


    @Override
    public int getFieldCount() {
        return holder.getFieldCount();
    }


    @Override
    public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        final Pair<AlgDataTypeField, Boolean> pair = holder.getFieldOrInsert( fieldName, caseSensitive );
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
    public PolyType getPolyType() {
        return PolyType.ROW;
    }


    @Override
    public AlgDataTypePrecedenceList getPrecedenceList() {
        return new PolyTypeExplicitPrecedenceList( ImmutableList.of() );
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
    public AlgDataTypeFamily getFamily() {
        return getPolyType().getFamily();
    }

}

