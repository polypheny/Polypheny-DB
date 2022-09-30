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
 */

package org.polypheny.db.adapter.cassandra.rules;


import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.adapter.cassandra.CassandraProject;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Rule to convert a {@link LogicalProject} to a {@link CassandraProject}.
 */
public class CassandraProjectRule extends CassandraConverterRule {

    CassandraProjectRule( CassandraConvention out, AlgBuilderFactory algBuilderFactory ) {
        super( Project.class, r -> true, Convention.NONE, out, algBuilderFactory, "CassandraProjectRule:" + out.getName() );
    }


    // TODO js: Reimplement the old checks!
    @Override
    public boolean matches( AlgOptRuleCall call ) {
        Project project = call.alg( 0 );
        for ( RexNode e : project.getProjects() ) {
            if ( !(e instanceof RexInputRef) && !(e instanceof RexLiteral) ) {
                if ( !((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) ) {
                    return false;
                }
            }
        }

        return true;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Project project = (Project) alg;
        final AlgTraitSet traitSet = project.getTraitSet().replace( out );

        boolean arrayValueProject = true;
        for ( RexNode e : project.getProjects() ) {
            if ( !((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) && !(e instanceof RexLiteral) ) {
                arrayValueProject = false;
            }
        }

        return new CassandraProject(
                project.getCluster(),
                traitSet,
                convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ),
                project.getProjects(),
                project.getRowType(),
                arrayValueProject );
    }

}
