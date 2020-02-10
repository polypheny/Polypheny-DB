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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.rules;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraProject;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} to a {@link CassandraProject}.
 */
public class CassandraProjectRule extends CassandraConverterRule {

    CassandraProjectRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Project.class, r -> true, Convention.NONE, out, relBuilderFactory, "CassandraProjectRule" );
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
