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


import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFamily;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypePrecedenceList;


/**
 * Abstract base class for SQL implementations of {@link RelDataType}.
 */
public abstract class AbstractPolyType extends RelDataTypeImpl implements Cloneable, Serializable {

    protected final PolyType typeName;
    protected boolean isNullable;


    /**
     * Creates an AbstractSqlType.
     *
     * @param typeName   Type name
     * @param isNullable Whether nullable
     * @param fields     Fields of type, or null if not a record type
     */
    protected AbstractPolyType( PolyType typeName, boolean isNullable, List<? extends RelDataTypeField> fields ) {
        super( fields );
        this.typeName = Objects.requireNonNull( typeName );
        this.isNullable = isNullable || (typeName == PolyType.NULL);
    }


    // implement RelDataType
    @Override
    public PolyType getPolyType() {
        return typeName;
    }


    // implement RelDataType
    @Override
    public boolean isNullable() {
        return isNullable;
    }


    // implement RelDataType
    @Override
    public RelDataTypeFamily getFamily() {
        return typeName.getFamily();
    }


    // implement RelDataType
    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        RelDataTypePrecedenceList list = PolyTypeExplicitPrecedenceList.getListForType( this );
        if ( list != null ) {
            return list;
        }
        return super.getPrecedenceList();
    }
}

