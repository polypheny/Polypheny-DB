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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import java.util.function.Predicate;


/**
 * Rule to convert a relational expression from {@link ElasticsearchRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class ElasticsearchToEnumerableConverterRule extends ConverterRule {

    static final ConverterRule INSTANCE = new ElasticsearchToEnumerableConverterRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates an ElasticsearchToEnumerableConverterRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    private ElasticsearchToEnumerableConverterRule( RelBuilderFactory relBuilderFactory ) {
        super( RelNode.class, (Predicate<RelNode>) r -> true, ElasticsearchRel.CONVENTION, EnumerableConvention.INSTANCE, relBuilderFactory, "ElasticsearchToEnumerableConverterRule" );
    }


    @Override
    public RelNode convert( RelNode relNode ) {
        RelTraitSet newTraitSet = relNode.getTraitSet().replace( getOutConvention() );
        return new ElasticsearchToEnumerableConverter( relNode.getCluster(), newTraitSet, relNode );
    }
}
