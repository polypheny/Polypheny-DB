/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Rule to convert a relational expression from {@link MongoRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class MongoToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new MongoToEnumerableConverterRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a MongoToEnumerableConverterRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public MongoToEnumerableConverterRule( RelBuilderFactory relBuilderFactory ) {
        super( RelNode.class, (Predicate<RelNode>) r -> true, MongoRel.CONVENTION, EnumerableConvention.INSTANCE, relBuilderFactory, "MongoToEnumerableConverterRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace( getOutConvention() );
        return new MongoToEnumerableConverter( rel.getCluster(), newTraitSet, rel );
    }
}

