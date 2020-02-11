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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import java.util.function.Predicate;


/**
 * Rule to convert a {@link LogicalProject} to an
 * {@link EnumerableProject}.
 */
class EnumerableProjectRule extends ConverterRule {

    EnumerableProjectRule() {
        super( LogicalProject.class,
                (Predicate<LogicalProject>) RelOptUtil::containsMultisetOrWindowedAgg,
                Convention.NONE,
                EnumerableConvention.INSTANCE,
                RelFactories.LOGICAL_BUILDER,
                "EnumerableProjectRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final LogicalProject project = (LogicalProject) rel;
        return EnumerableProject.create(
                RelOptRule.convert( project.getInput(), project.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) ),
                project.getProjects(),
                project.getRowType() );
    }
}

