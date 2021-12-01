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

package org.polypheny.db.plan.hep;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.algebra.AlgNode;


/**
 * HepRuleCall implements {@link AlgOptRuleCall} for a {@link HepPlanner}. It remembers transformation results so that the planner can choose which one (if any) should replace the original expression.
 */
public class HepRuleCall extends AlgOptRuleCall {

    private List<AlgNode> results;


    HepRuleCall( AlgOptPlanner planner, AlgOptRuleOperand operand, AlgNode[] rels, Map<AlgNode, List<AlgNode>> nodeChildren, List<AlgNode> parents ) {
        super( planner, operand, rels, nodeChildren, parents );
        results = new ArrayList<>();
    }


    // implement RelOptRuleCall
    @Override
    public void transformTo( AlgNode alg, Map<AlgNode, AlgNode> equiv ) {
        final AlgNode rel0 = algs[0];
        AlgOptUtil.verifyTypeEquivalence( rel0, alg, rel0 );
        results.add( alg );
        alg( 0 ).getCluster().invalidateMetadataQuery();
    }


    List<AlgNode> getResults() {
        return results;
    }
}

