/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.rex;

import org.polypheny.db.algebra.type.AlgDataType;

import java.util.Objects;

/**
 * Named dynamic parameter reference in a row-expression.
 */
public class RexNamedDynamicParam extends RexDynamicParam {

    /**
     * Creates a named dynamic parameter.
     *
     * @param type inferred type of parameter
     * @param index 0-based index of dynamic parameter in statement
     */
    public RexNamedDynamicParam(AlgDataType type, long index, String name) {
        super( type, index, ":" + name);
    }


//    @Override
//    public Kind getKind() {
//        return Kind.DYNAMIC_PARAM;
//    }
//

    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof RexNamedDynamicParam
                && digest.equals( ((RexNamedDynamicParam) obj).digest )
                && type.equals( ((RexNamedDynamicParam) obj).type )
                && getIndex() == ((RexNamedDynamicParam) obj).getIndex()
                && name.equals(((RexNamedDynamicParam) obj).name);
    }


    @Override
    public int hashCode() {
        return Objects.hash( digest, type, getIndex(), name );
    }

}

