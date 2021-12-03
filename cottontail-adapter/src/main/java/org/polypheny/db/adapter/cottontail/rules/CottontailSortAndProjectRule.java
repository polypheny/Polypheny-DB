/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.adapter.cottontail.algebra.CottontailSortAndProject;
import org.polypheny.db.adapter.cottontail.algebra.SortAndProject;
import org.polypheny.db.sql.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.sql.fun.SqlDistanceFunction;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


public class CottontailSortAndProjectRule extends AlgOptRule {

    protected final Convention out;


    CottontailSortAndProjectRule( CottontailConvention out, AlgBuilderFactory algBuilderFactory ) {
        super( operand( Sort.class, operand( Project.class, any() ) ), algBuilderFactory, "CottontailSortAndProjectRule" + out.getName() );
        this.out = out;
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );

        if ( !(call.alg( 1 ) instanceof Project) ) {
            return false;
        }

        Project project = call.alg( 1 );

        if ( !project.getInput().getConvention().equals( this.out ) ) {
            return false;
        }

        // Projection checks
        Project innerProject = getUnderlyingProject( (AlgSubset) project.getInput(), this.out );

        if ( innerProject != null ) {
            return false;
        }

        boolean containsInputRefs = false;
        boolean containsValueProjects = false;
        boolean foundKnnFunction = false;
        int knnColumn = -1;

        List<RexNode> projects = project.getProjects();
        for ( int i = 0, projectsSize = projects.size(); i < projectsSize; i++ ) {
            RexNode e = projects.get( i );

            if ( e instanceof RexInputRef ) {
                containsInputRefs = true;
            } else if ( (e instanceof RexLiteral) || (e instanceof RexDynamicParam) || ((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) ) {
                containsValueProjects = true;
            } else if ( (e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlDistanceFunction) ) {
                RexCall rexCall = (RexCall) e;
                if ( !foundKnnFunction ) {

                    if ( (CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( rexCall.getOperands().get( 0 ).getType().getComponentType().getPolyType() )) ) {
                        foundKnnFunction = true;
                        knnColumn = i;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        if ( !(containsInputRefs && foundKnnFunction && !containsValueProjects) ) {
            return false;
        }

        // Sort checks
        if ( sort.getCollation().getFieldCollations().size() != 1 ) {
            return false;
        }

        AlgFieldCollation collation = sort.getCollation().getFieldCollations().get( 0 );

        if ( collation.getFieldIndex() != knnColumn ) {
            return false;
        }

        if ( sort.fetch == null ) {
            return false;
        }

        return collation.getDirection() == Direction.ASCENDING;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        Project project = call.alg( 1 );

        final AlgTraitSet traitSet = sort.getTraitSet().replace( out );
        final AlgNode input;
        final AlgTraitSet inputTraitSet = project.getInput().getTraitSet().replace( out );
        input = convert( project.getInput(), inputTraitSet );

        boolean arrayValueProject = true;
        for ( RexNode e : project.getProjects() ) {
            if ( !((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) && !(e instanceof RexLiteral) && !(e instanceof RexDynamicParam) ) {
                arrayValueProject = false;
            }
        }

        SortAndProject sortAndProject = new CottontailSortAndProject(
                sort.getCluster(),
                traitSet,
                input,
                sort.getCollation(),
                sort.offset,
                sort.fetch,
                project.getProjects(),
                project.getRowType(),
                null,
                arrayValueProject );

        call.transformTo( sortAndProject );
    }


    /**
     * Finds the underlying {@link Project} of the subset.
     *
     * @param relSubset the subset.
     * @return the {@link Project} or <code>null</code> if not found.
     */
    public static Project getUnderlyingProject( AlgSubset relSubset, Convention targetConvention ) {
        return getUnderlyingProject( relSubset.getAlgList(), targetConvention );
    }


    private static Project getUnderlyingProject( List<AlgNode> rels, Convention targetConvention ) {
        Set<AlgNode> alreadyChecked = new HashSet<>();
        Deque<AlgNode> innerLevel = new LinkedList<>();

        innerLevel.addAll( rels );

        while ( !innerLevel.isEmpty() ) {
            AlgNode algNode = innerLevel.pop();
            alreadyChecked.add( algNode );
            if ( algNode instanceof Project ) {
//                if ( ((Project) algNode).getInput().getConvention().equals( targetConvention ) ) {
                return (Project) algNode;
//                }
            } else {
                for ( AlgNode innerNode : algNode.getInputs() ) {
                    if ( innerNode instanceof AlgSubset ) {
                        for ( AlgNode possibleNewRel : ((AlgSubset) innerNode).getAlgList() ) {
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
