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
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;


/**
 * Base Class for relations with three or more inputs
 */
abstract class MutableMultiAlg extends MutableAlg {

    protected final List<MutableAlg> inputs;


    protected MutableMultiAlg( AlgCluster cluster, AlgDataType rowType, MutableAlgType type, List<MutableAlg> inputs ) {
        super( cluster, rowType, type );
        this.inputs = ImmutableList.copyOf( inputs );
        for ( Ord<MutableAlg> input : Ord.zip( inputs ) ) {
            input.e.parent = this;
            input.e.ordinalInParent = input.i;
        }
    }


    @Override
    public void setInput( int ordinalInParent, MutableAlg input ) {
        inputs.set( ordinalInParent, input );
        if ( input != null ) {
            input.parent = this;
            input.ordinalInParent = ordinalInParent;
        }
    }


    @Override
    public List<MutableAlg> getInputs() {
        return inputs;
    }


    @Override
    public void childrenAccept( MutableAlgVisitor visitor ) {
        for ( MutableAlg input : inputs ) {
            visitor.visit( input );
        }
    }


    protected List<MutableAlg> cloneChildren() {
        return Lists.transform( inputs, MutableAlg::clone );
    }

}

