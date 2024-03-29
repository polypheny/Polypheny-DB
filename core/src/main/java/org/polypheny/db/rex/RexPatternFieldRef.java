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

package org.polypheny.db.rex;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;


/**
 * Variable which references a field of an input algebra expression
 */
public class RexPatternFieldRef extends RexIndexRef {

    private final String alpha;


    public RexPatternFieldRef( String alpha, int index, AlgDataType type ) {
        super( index, type );
        this.alpha = alpha;
        digest = alpha + ".$" + index;
    }


    public String getAlpha() {
        return alpha;
    }


    public static RexPatternFieldRef of( String alpha, int index, AlgDataType type ) {
        return new RexPatternFieldRef( alpha, index, type );
    }


    public static RexPatternFieldRef of( String alpha, RexIndexRef ref ) {
        return new RexPatternFieldRef( alpha, ref.getIndex(), ref.getType() );
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitPatternFieldRef( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitPatternFieldRef( this, arg );
    }


    @Override
    public Kind getKind() {
        return Kind.PATTERN_INPUT_REF;
    }

}

