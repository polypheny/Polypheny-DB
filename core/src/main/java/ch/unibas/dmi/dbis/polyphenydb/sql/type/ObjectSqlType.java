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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeComparability;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import java.util.List;


/**
 * ObjectSqlType represents an SQL structured user-defined type.
 */
public class ObjectSqlType extends AbstractSqlType {

    private final SqlIdentifier sqlIdentifier;

    private final RelDataTypeComparability comparability;

    private RelDataTypeFamily family;


    /**
     * Constructs an object type. This should only be called from a factory method.
     *
     * @param typeName SqlTypeName for this type (either Distinct or Structured)
     * @param sqlIdentifier identifier for this type
     * @param nullable whether type accepts nulls
     * @param fields object attribute definitions
     */
    public ObjectSqlType( SqlTypeName typeName, SqlIdentifier sqlIdentifier, boolean nullable, List<? extends RelDataTypeField> fields, RelDataTypeComparability comparability ) {
        super( typeName, nullable, fields );
        this.sqlIdentifier = sqlIdentifier;
        this.comparability = comparability;
        computeDigest();
    }


    public void setFamily( RelDataTypeFamily family ) {
        this.family = family;
    }


    // implement RelDataType
    public RelDataTypeComparability getComparability() {
        return comparability;
    }


    // override AbstractSqlType
    public SqlIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }


    // override AbstractSqlType
    public RelDataTypeFamily getFamily() {
        // each UDT is in its own lonely family, until one day when we support inheritance (at which time also need to implement getPrecedenceList).
        return family;
    }


    // implement RelDataTypeImpl
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        // TODO jvs 10-Feb-2005:  proper quoting; dump attributes withDetail?
        sb.append( "ObjectSqlType(" );
        sb.append( sqlIdentifier.toString() );
        sb.append( ")" );
    }
}

