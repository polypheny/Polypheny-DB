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


import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.type.PolyType;


/**
 * {@link AlgRecordType} represents a structured type having named fields.
 */
public class AlgRecordType extends AlgDataTypeImpl implements Serializable {

    /**
     * Name resolution policy; usually {@link StructKind#FULLY_QUALIFIED}.
     */
    private final StructKind kind;


    /**
     * Creates a <code>RecordType</code>. This should only be called from a factory method.
     */
    public AlgRecordType( StructKind kind, List<AlgDataTypeField> fields ) {
        super( fields );
        this.kind = Objects.requireNonNull( kind );
        computeDigest();
    }


    public AlgRecordType( List<AlgDataTypeField> fields ) {
        this( StructKind.FULLY_QUALIFIED, fields );
    }


    @Override
    public PolyType getPolyType() {
        return PolyType.ROW;
    }


    @Override
    public AlgDataType asRelational() {
        return this;
    }


    @Override
    public int getPrecision() {
        // REVIEW angel: Put in fake implementation for precision
        return 0;
    }


    @Override
    public StructKind getStructKind() {
        return kind;
    }


    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        sb.append( "RecordType" );
        switch ( kind ) {
            case PEEK_FIELDS:
                sb.append( ":peek" );
                break;
            case PEEK_FIELDS_DEFAULT:
                sb.append( ":peek_default" );
                break;
            case PEEK_FIELDS_NO_EXPAND:
                sb.append( ":peek_no_expand" );
                break;
        }
        sb.append( "(" );
        for ( Ord<AlgDataTypeField> ord : Ord.zip( fields ) ) {
            if ( ord.i > 0 ) {
                sb.append( ", " );
            }
            AlgDataTypeField field = ord.e;
            if ( withDetail ) {
                sb.append( field.getType().getFullTypeString() );
            } else {
                sb.append( field.getType().toString() );
            }
            sb.append( " " );
            sb.append( field.getName() );
        }
        sb.append( ")" );
    }


    /**
     * Per {@link Serializable} API, provides a replacement object to be written during serialization.
     * <p>
     * This implementation converts this RelRecordType into a SerializableRelRecordType, whose <code>readResolve</code>
     * method converts it back to a RelRecordType during deserialization.
     */
    @Serial
    private Object writeReplace() {
        return new SerializableAlgRecordType( fields );
    }


    public boolean isPrepared() {
        return fields.size() == 1 && fields.get( 0 ).getName().equals( "ZERO" ) && fields.get( 0 ).getType().getPolyType() == PolyType.INTEGER;
    }


    /**
     * Skinny object which has the same information content as a {@link AlgRecordType} but skips redundant stuff like
     * digest and the immutable list.
     */
    private record SerializableAlgRecordType( List<AlgDataTypeField> fields ) implements Serializable {


        /**
         * Per {@link Serializable} API. See {@link AlgRecordType#writeReplace()}.
         */
        @Serial
        private Object readResolve() {
            return new AlgRecordType( fields );
        }

    }

}
