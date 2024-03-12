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

package org.polypheny.db.algebra.mutable;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;


/**
 * Mutable equivalent of {@link BiAlg}.
 */
abstract class MutableBiAlg extends MutableAlg {

    protected MutableAlg left;
    protected MutableAlg right;


    protected MutableBiAlg( MutableAlgType type, AlgCluster cluster, AlgDataType rowType, MutableAlg left, MutableAlg right ) {
        super( cluster, rowType, type );
        this.left = left;
        left.parent = this;
        left.ordinalInParent = 0;

        this.right = right;
        right.parent = this;
        right.ordinalInParent = 1;
    }


    @Override
    public void setInput( int ordinalInParent, MutableAlg input ) {
        if ( ordinalInParent > 1 ) {
            throw new IllegalArgumentException();
        }
        if ( ordinalInParent == 0 ) {
            this.left = input;
        } else {
            this.right = input;
        }
        if ( input != null ) {
            input.parent = this;
            input.ordinalInParent = ordinalInParent;
        }
    }


    @Override
    public List<MutableAlg> getInputs() {
        return ImmutableList.of( left, right );
    }


    public MutableAlg getLeft() {
        return left;
    }


    public MutableAlg getRight() {
        return right;
    }


    @Override
    public void childrenAccept( MutableAlgVisitor visitor ) {
        visitor.visit( left );
        visitor.visit( right );
    }

}

