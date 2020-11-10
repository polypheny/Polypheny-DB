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

package org.polypheny.db.adapter.cottontail.rules;


import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.rel.CottontailProject;
import org.polypheny.db.adapter.cottontail.rel.CottontailSort;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.RelSubset;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelFieldCollation.Direction;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.fun.SqlKnnFunction;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.Pair;


public class CottontailSortRule extends CottontailConverterRule {

    CottontailSortRule( CottontailConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Sort.class, r -> true, Convention.NONE, out, relBuilderFactory, "CottontailSortRule" + out.getName() );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        final Sort sort = call.rel( 0 );

        Project project = getUnderlyingProject( (RelSubset) sort.getInput(), this.out );

        if ( project == null ) {
            return false;
        }

        int knnColumnIndex = -1;

        List<Pair<RexNode, String>> namedProjects = project.getNamedProjects();
        for ( int i = 0; i < namedProjects.size(); i++ ) {
            Pair<RexNode, String> pair = namedProjects.get( i );
            if ( pair.left instanceof RexCall && (((RexCall) pair.left).getOperator() instanceof SqlKnnFunction) ) {
                knnColumnIndex = i;
            }
        }

        if ( knnColumnIndex == -1 ) {
            return false;
        }

        if ( sort.getCollation().getFieldCollations().size() != 1 ) {
            return false;
        }

        RelFieldCollation collation = sort.getCollation().getFieldCollations().get( 0 );

        if ( collation.getFieldIndex() != knnColumnIndex ) {
            return false;
        }

        if ( collation.getDirection() != Direction.ASCENDING ) {
            return false;
        }


        return true;

//        return super.matches( call );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        Sort sort = (Sort) rel;
//        final RelTraitSet traitSet = sort.getInput().getTraitSet().replace( out );
        final RelTraitSet traitSet = sort.getTraitSet().replace( out );
        final RelNode input;
        final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace( out );
        input = convert( sort.getInput(), inputTraitSet );

//        final RelTraitSet traitSet = sort.getTraitSet().replace( out ).replace( sort.getCollation() );
        return new CottontailSort( sort.getCluster(), traitSet, input, sort.getCollation(), sort.offset, sort.fetch );
//        return new CottontailSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( out ) ), sort.getCollation(), sort.offset, sort.fetch );
//        return new CottontailSort( sort.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( RelCollations.EMPTY ) ), sort.getCollation(), sort.offset, sort.fetch );
    }


    /**
     * Finds the underlying {@link Project} of the subset.
     *
     * @param relSubset the subset.
     * @return the {@link Project} or <code>null</code> if not found.
     */
    public static Project getUnderlyingProject( RelSubset relSubset, Convention targetConvention ) {
        return getUnderlyingProject( relSubset.getRelList(), targetConvention );
    }


    private static Project getUnderlyingProject( List<RelNode> rels, Convention targetConvention ) {
        Set<RelNode> alreadyChecked = new HashSet<>();
        Deque<RelNode> innerLevel = new LinkedList<>();

        innerLevel.addAll( rels );

        while ( !innerLevel.isEmpty() ) {
            RelNode relNode = innerLevel.pop();
            alreadyChecked.add( relNode );
            if ( relNode instanceof Project ) {
//                if ( relNode.getConvention().equals( targetConvention ) ) {
                    return (Project) relNode;
//                }
            } else {
                for ( RelNode innerNode : relNode.getInputs() ) {
                    if ( innerNode instanceof RelSubset ) {
                        for ( RelNode possibleNewRel : ((RelSubset) innerNode).getRelList() ) {
                            if ( !alreadyChecked.contains( possibleNewRel ) ) {
                                innerLevel.addLast( possibleNewRel );
                            }
                        }
                    }
                }
            }
        }

        return null;
    }
}
