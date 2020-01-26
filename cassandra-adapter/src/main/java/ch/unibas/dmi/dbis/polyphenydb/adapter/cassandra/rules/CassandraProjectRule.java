/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
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
        return new CassandraProject( project.getCluster(), traitSet, convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ), project.getProjects(), project.getRowType() );
    }
}
