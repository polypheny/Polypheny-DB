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

package org.polypheny.db.algebra.enumerable;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert a {@link LogicalRelProject} to an
 * {@link EnumerableProject}.
 */
public class EnumerableProjectRule extends ConverterRule {

    EnumerableProjectRule() {
        super(
                LogicalRelProject.class,
                AlgOptUtil::containsMultisetOrWindowedAgg,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER,
                "EnumerableProjectRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalRelProject project = (LogicalRelProject) alg;
        return EnumerableProject.create(
                AlgOptRule.convert( project.getInput(), project.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) ),
                project.getProjects(),
                project.getTupleType() );
    }

}

