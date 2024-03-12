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

package org.polypheny.db.adapter.csv;

import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that projects from a {@link CsvScan} relScan just the columns needed to satisfy a projection. If the
 * projection's expressions are trivial, the projection is removed.
 */
public class CsvProjectScanRule extends AlgOptRule {

    public static final CsvProjectScanRule INSTANCE = new CsvProjectScanRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a CsvProjectScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public CsvProjectScanRule( AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalRelProject.class, operand( CsvScan.class, none() ) ),
                algBuilderFactory,
                "CsvProjectScanRule"
        );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalRelProject project = call.alg( 0 );
        final CsvScan scan = call.alg( 1 );
        int[] fields = getProjectFields( project.getProjects() );
        if ( fields == null ) {
            // Project contains expressions more complex than just field references.
            return;
        }

        call.transformTo( new CsvScan( scan.getCluster(), scan.getEntity(), scan.csvTable, fields ) );
    }


    private int[] getProjectFields( List<RexNode> exps ) {
        final int[] fields = new int[exps.size()];
        for ( int i = 0; i < exps.size(); i++ ) {
            final RexNode exp = exps.get( i );
            if ( exp instanceof RexIndexRef ) {
                fields[i] = ((RexIndexRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }

}

