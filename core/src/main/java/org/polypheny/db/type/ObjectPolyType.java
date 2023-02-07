/*
 * Copyright 2019-2023 The Polypheny Project
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


import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataTypeComparability;
import org.polypheny.db.algebra.type.AlgDataTypeFamily;
import org.polypheny.db.algebra.type.AlgDataTypeField;

import java.util.List;


/**
 * ObjectSqlType represents an SQL structured user-defined type.
 */
public class ObjectPolyType extends AbstractPolyType {

    @Getter
    private final AlgDataTypeComparability comparability;

    @Getter
    private AlgDataTypeFamily family;


    /**
     * Constructs an object type. This should only be called from a factory method.
     *
     * @param typeName PolyType for this type (either Distinct or Structured)
     * @param nullable whether type accepts nulls
     * @param fields object attribute definitions
     */
    public ObjectPolyType(
            PolyType typeName,
            boolean nullable,
            List<? extends AlgDataTypeField> fields,
            AlgDataTypeComparability comparability ) {
        super( typeName, nullable, fields );
        this.comparability = comparability;
        computeDigest();
    }


    public void setFamily( AlgDataTypeFamily family ) {
        this.family = family;
    }


    // implement RelDataTypeImpl
    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        // TODO: proper quoting; dump attributes withDetail?
        sb.append( "ObjectSqlType(" );
        sb.append( ")" );
    }

}

