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
import org.apache.calcite.linq4j.Ord;


/**
 * Type of the cartesian product of two or more sets of records.
 * <p>
 * Its fields are those of its constituent records, but unlike a {@link AlgRecordType}, those fields' names are not
 * necessarily distinct.
 */
public class AlgCrossType extends AlgDataTypeImpl {

    public final ImmutableList<AlgDataType> types;


    /**
     * Creates a cartesian product type. This should only be called from a factory method.
     */
    public AlgCrossType( List<AlgDataType> types, List<AlgDataTypeField> fields ) {
        super( fields );
        this.types = ImmutableList.copyOf( types );
        assert !types.isEmpty();
        for ( AlgDataType type : types ) {
            assert !(type instanceof AlgCrossType);
        }
        computeDigest();
    }


    @Override
    public boolean isStruct() {
        return false;
    }


    @Override
    public List<AlgDataTypeField> getFields() {
        return fields;
    }


    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        sb.append( "CrossType(" );
        for ( Ord<AlgDataType> type : Ord.zip( types ) ) {
            if ( type.i > 0 ) {
                sb.append( ", " );
            }
            if ( withDetail ) {
                sb.append( type.e.getFullTypeString() );
            } else {
                sb.append( type.e.toString() );
            }
        }
        sb.append( ")" );
    }


    public AlgDataType getKeyType() {
        // this is not a map type
        return null;
    }


    public AlgDataType getValueType() {
        // this is not a map type
        return null;
    }

}

