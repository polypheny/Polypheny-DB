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

package org.polypheny.db.adapter.mongodb;


import java.util.function.Predicate;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to convert a relational expression from {@link MongoAlg#CONVENTION} to {@link EnumerableConvention}.
 */
public class MongoToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new MongoToEnumerableConverterRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a MongoToEnumerableConverterRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public MongoToEnumerableConverterRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                AlgNode.class,
                (Predicate<AlgNode>) r -> true,
                MongoAlg.CONVENTION,
                EnumerableConvention.INSTANCE,
                algBuilderFactory,
                "MongoToEnumerableConverterRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        AlgTraitSet newTraitSet = alg.getTraitSet().replace( getOutTrait() );
        return new MongoToEnumerableConverter( alg.getCluster(), newTraitSet, alg );
    }

}

