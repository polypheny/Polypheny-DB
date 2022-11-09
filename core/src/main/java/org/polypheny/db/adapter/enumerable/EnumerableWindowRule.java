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

package org.polypheny.db.adapter.enumerable;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalWindow;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert a {@link LogicalWindow} to an {@link org.polypheny.db.adapter.enumerable.EnumerableWindow}.
 */
class EnumerableWindowRule extends ConverterRule {

    EnumerableWindowRule() {
        super( LogicalWindow.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableWindowRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalWindow winAgg = (LogicalWindow) alg;
        final AlgTraitSet traitSet = winAgg.getTraitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode child = winAgg.getInput();
        final AlgNode convertedChild = convert( child, child.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        return new EnumerableWindow( alg.getCluster(), traitSet, convertedChild, winAgg.getConstants(), winAgg.getRowType(), winAgg.groups );
    }

}

