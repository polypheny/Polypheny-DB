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
 */

package org.polypheny.db.adapter.cassandra.rules;


import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.adapter.cassandra.CassandraProject;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilderFactory;


/**
 * Rule to convert a {@link org.polypheny.db.rel.logical.LogicalProject} to a {@link CassandraProject}.
 */
public class CassandraProjectRule extends CassandraConverterRule {

    CassandraProjectRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Project.class, r -> true, Convention.NONE, out, relBuilderFactory, "CassandraProjectRule:" + out.getName() );
    }


    // TODO js: Reimplement the old checks!
    @Override
    public boolean matches( RelOptRuleCall call ) {
        Project project = call.rel( 0 );
        for ( RexNode e : project.getProjects() ) {
            if ( !(e instanceof RexInputRef) && !(e instanceof RexLiteral) ) {
                return false;
            }
        }

        return true;
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final Project project = (Project) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace( out );
        return new CassandraProject(
                project.getCluster(),
                traitSet,
                convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ),
                project.getProjects(),
                project.getRowType() );
    }
}
