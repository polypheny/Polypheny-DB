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


import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Ord;


/**
 * RelRecordType represents a structured type having named fields.
 */
public class RelRecordType extends RelDataTypeImpl implements Serializable {

    /**
     * Name resolution policy; usually {@link StructKind#FULLY_QUALIFIED}.
     */
    private final StructKind kind;


    /**
     * Creates a <code>RecordType</code>. This should only be called from a factory method.
     */
    public RelRecordType( StructKind kind, List<RelDataTypeField> fields ) {
        super( fields );
        this.kind = Objects.requireNonNull( kind );
        computeDigest();
    }


    public RelRecordType( List<RelDataTypeField> fields ) {
        this( StructKind.FULLY_QUALIFIED, fields );
    }


    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ROW;
    }


    @Override
    public boolean isNullable() {
        return false;
    }


    @Override
    public int getPrecision() {
        // REVIEW: angel 18-Aug-2005 Put in fake implementation for precision
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
        for ( Ord<RelDataTypeField> ord : Ord.zip( fieldList ) ) {
            if ( ord.i > 0 ) {
                sb.append( ", " );
            }
            RelDataTypeField field = ord.e;
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
     *
     * This implementation converts this RelRecordType into a SerializableRelRecordType, whose <code>readResolve</code> method converts it back to a RelRecordType during deserialization.
     */
    private Object writeReplace() {
        return new SerializableRelRecordType( fieldList );
    }


    /**
     * Skinny object which has the same information content as a {@link RelRecordType} but skips redundant stuff like digest and the immutable list.
     */
    private static class SerializableRelRecordType implements Serializable {

        private List<RelDataTypeField> fields;


        private SerializableRelRecordType( List<RelDataTypeField> fields ) {
            this.fields = fields;
        }


        /**
         * Per {@link Serializable} API. See {@link RelRecordType#writeReplace()}.
         */
        private Object readResolve() {
            return new RelRecordType( fields );
        }
    }
}
