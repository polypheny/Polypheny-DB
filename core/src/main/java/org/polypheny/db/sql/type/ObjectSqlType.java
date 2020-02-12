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

package org.polypheny.db.sql.type;


import org.polypheny.db.rel.type.RelDataTypeComparability;
import org.polypheny.db.rel.type.RelDataTypeFamily;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.SqlIdentifier;
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
    @Override
    public RelDataTypeComparability getComparability() {
        return comparability;
    }


    // override AbstractSqlType
    @Override
    public SqlIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }


    // override AbstractSqlType
    @Override
    public RelDataTypeFamily getFamily() {
        // each UDT is in its own lonely family, until one day when we support inheritance (at which time also need to implement getPrecedenceList).
        return family;
    }


    // implement RelDataTypeImpl
    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        // TODO jvs 10-Feb-2005:  proper quoting; dump attributes withDetail?
        sb.append( "ObjectSqlType(" );
        sb.append( sqlIdentifier.toString() );
        sb.append( ")" );
    }
}

