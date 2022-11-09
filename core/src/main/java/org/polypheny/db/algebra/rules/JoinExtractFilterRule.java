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

package org.polypheny.db.algebra.rules;


import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to convert an {@link LogicalJoin inner join} to a {@link LogicalFilter filter} on top of a {@link LogicalJoin cartesian inner join}.
 *
 * One benefit of this transformation is that after it, the join condition can be combined with conditions and expressions
 * above the join. It also makes the <code>FennelCartesianJoinRule</code> applicable.
 *
 * The constructor is parameterized to allow any sub-class of {@link Join}, not just {@link LogicalJoin}.
 */
public final class JoinExtractFilterRule extends AbstractJoinExtractFilterRule {

    /**
     * The singleton.
     */
    public static final JoinExtractFilterRule INSTANCE = new JoinExtractFilterRule( LogicalJoin.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinExtractFilterRule.
     */
    public JoinExtractFilterRule( Class<? extends Join> clazz, AlgBuilderFactory algBuilderFactory ) {
        super( AlgOptRule.operand( clazz, AlgOptRule.any() ), algBuilderFactory, null );
    }

}

