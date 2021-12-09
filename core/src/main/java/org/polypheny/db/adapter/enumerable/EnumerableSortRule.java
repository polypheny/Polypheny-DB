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

package org.polypheny.db.adapter.enumerable;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert an {@link Sort} to an {@link EnumerableSort}.
 */
class EnumerableSortRule extends ConverterRule {

    EnumerableSortRule() {
        super( Sort.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableSortRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Sort sort = (Sort) alg;
        if ( sort.offset != null || sort.fetch != null ) {
            return null;
        }
        final AlgNode input = sort.getInput();
        return EnumerableSort.create(
                convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) ),
                sort.getCollation(),
                null,
                null );
    }

}

