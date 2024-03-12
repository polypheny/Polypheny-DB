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
 */

package org.polypheny.db.adapter.cottontail.rules;

import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.algebra.CottontailProject;
import org.polypheny.db.adapter.cottontail.algebra.CottontailToEnumerableConverter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.nodes.ArrayValueConstructor;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.language.fun.SqlDistanceFunction;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.UnsupportedRexCallVisitor;


public class CottontailProjectRule extends CottontailConverterRule {

    CottontailProjectRule( AlgBuilderFactory algBuilderFactory ) {
        super( Project.class,
                p -> !DocumentRules.containsDocument( p ) && !UnsupportedRexCallVisitor.containsModelItem( p.getProjects() ),
                Convention.NONE,
                CottontailConvention.INSTANCE,
                algBuilderFactory,
                CottontailProjectRule.class.getSimpleName() );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        Project project = call.alg( 0 );
        /*if ( containsInnerProject( project ) ) {
            return super.matches( call );
        }*/

        boolean containsInputRefs = false;
        boolean containsValueProjects = false;

        List<RexNode> projects = project.getProjects();
        for ( RexNode e : projects ) {
            if ( e instanceof RexIndexRef ) {
                containsInputRefs = true;
            } else if ( (e instanceof RexLiteral) || (e instanceof RexDynamicParam) || (e instanceof RexCall rexCall && rexCall.getOperator() instanceof ArrayValueConstructor) ) {
                containsValueProjects = true;
            } else if ( e instanceof RexCall rexCall && rexCall.getOperator() instanceof SqlDistanceFunction ) {
                if ( !(CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( rexCall.getOperands().get( 0 ).getType().getComponentType().getPolyType() )) ) {
                    return false;
                }
                for ( RexNode node : rexCall.getOperands() ) {
                    if ( node instanceof RexCall ) {
                        return false; /* TODO: Nested function calls in NNS context are currently not supported. */
                    }
                }
            } else {
                return false;
            }
        }
        return containsInputRefs || containsValueProjects;
    }


    private boolean isPreparedDml( Project project ) {
        // only manually prepared DML statements are supported for operators than index references and knn functions
        return project.getInput() instanceof AlgSubset subset && subset.getOriginal() instanceof Values;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Project project = (Project) alg;
        final AlgTraitSet traitSet = project.getTraitSet().replace( out );
        boolean arrayValueProject = true;
        for ( RexNode e : project.getProjects() ) {
            if ( !(e instanceof RexCall call && call.getOperator() instanceof SqlArrayValueConstructor)
                    && !(e instanceof RexLiteral)
                    && !(e instanceof RexDynamicParam) ) {
                arrayValueProject = false;
                break;
            }
        }

        return new CottontailProject(
                project.getCluster(),
                traitSet,
                convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ),
                project.getProjects(),
                project.getTupleType(),
                arrayValueProject
        );
    }


    private static boolean containsInnerProject( Project project ) {
        List<AlgNode> algs = ((AlgSubset) project.getInput()).getAlgList();
        Set<AlgNode> alreadyChecked = new HashSet<>();
        Deque<AlgNode> innerLevel = new LinkedList<>( algs );

        while ( !innerLevel.isEmpty() ) {
            AlgNode algNode = innerLevel.pop();
            alreadyChecked.add( algNode );
            if ( algNode instanceof Project ) {
                return true;
            } else {
                for ( AlgNode innerNode : algNode.getInputs() ) {
                    if ( innerNode instanceof AlgSubset ) {
                        for ( AlgNode possibleNewAlg : ((AlgSubset) innerNode).getAlgList() ) {
                            if ( !alreadyChecked.contains( possibleNewAlg ) ) {
                                innerLevel.addLast( possibleNewAlg );
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

}
