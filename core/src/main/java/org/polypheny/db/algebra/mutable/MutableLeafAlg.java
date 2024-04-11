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
import org.polypheny.db.algebra.AlgNode;


/**
 * Abstract base class for implementations of {@link MutableAlg} that have no inputs.
 */
abstract class MutableLeafAlg extends MutableAlg {

    protected final AlgNode alg;


    protected MutableLeafAlg( MutableAlgType type, AlgNode alg ) {
        super( alg.getCluster(), alg.getTupleType(), type );
        this.alg = alg;
    }


    @Override
    public void setInput( int ordinalInParent, MutableAlg input ) {
        throw new IllegalArgumentException();
    }


    @Override
    public List<MutableAlg> getInputs() {
        return ImmutableList.of();
    }


    @Override
    public void childrenAccept( MutableAlgVisitor visitor ) {
        // no children - nothing to do
    }

}

