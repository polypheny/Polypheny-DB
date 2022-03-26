/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.util.Objects;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;


/**
 * Dynamic parameter reference in a row-expression.
 */
public class RexDynamicParam extends RexVariable {

    private final long index;


    /**
     * Creates a dynamic parameter.
     *
     * @param type inferred type of parameter
     * @param index 0-based index of dynamic parameter in statement
     */
    public RexDynamicParam( AlgDataType type, long index ) {
        super( "?" + index + ":" + type.getPolyType().getName(), type );
        // without this change, during optimization two nodes,
        // which are parameterized can result in the same digest, this leads to errors, on set retrieval {VulcanoPlanner#mapDigestToRel}
        // e.g. project(id File) -> $f0=?0  project(name VARCHAR(30)) -> $f0=?0
        this.index = index;
    }


    @Override
    public Kind getKind() {
        return Kind.DYNAMIC_PARAM;
    }


    public long getIndex() {
        return index;
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitDynamicParam( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitDynamicParam( this, arg );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof RexDynamicParam
                && digest.equals( ((RexDynamicParam) obj).digest )
                && type.equals( ((RexDynamicParam) obj).type )
                && index == ((RexDynamicParam) obj).index;
    }


    @Override
    public int hashCode() {
        return Objects.hash( digest, type, index );
    }

}

