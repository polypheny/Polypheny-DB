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
import org.polypheny.db.adapter.cottontail.rel.CottontailProject;
import org.polypheny.db.document.rules.DocumentRules;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.RelSubset;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.fun.SqlDistanceFunction;
import org.polypheny.db.tools.RelBuilderFactory;


public class CottontailProjectRule extends CottontailConverterRule {

    CottontailProjectRule( CottontailConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Project.class, p -> !DocumentRules.containsDocument( p ), Convention.NONE, out, relBuilderFactory, "CottontailProjectRule:" + out.getName() );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        Project project = call.rel( 0 );
        if ( containsInnerProject( project ) ) {
            return super.matches( call );
        }

        boolean containsInputRefs = false;
        boolean containsValueProjects = false;

        List<RexNode> projects = project.getProjects();
        for ( RexNode e : projects ) {
            if ( e instanceof RexInputRef ) {
                containsInputRefs = true;
            } else if ( (e instanceof RexLiteral) || (e instanceof RexDynamicParam) || ((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) ) {
                containsValueProjects = true;
            } else if ( (e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlDistanceFunction) ) {
                RexCall rexCall = (RexCall) e;
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


    @Override
    public RelNode convert( RelNode rel ) {
        final Project project = (Project) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace( out );
        boolean arrayValueProject = true;
        for ( RexNode e : project.getProjects() ) {
            if ( !((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor))
                    && !(e instanceof RexLiteral) && !(e instanceof RexDynamicParam) ) {
                arrayValueProject = false;
                break;
            }
        }

        return new CottontailProject(
                project.getCluster(),
                traitSet,
                convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ),
                project.getProjects(),
                project.getRowType(),
                arrayValueProject
        );
    }


    private static boolean containsInnerProject( Project project ) {
        List<RelNode> rels = ((RelSubset) project.getInput()).getRelList();
        Set<RelNode> alreadyChecked = new HashSet<>();
        Deque<RelNode> innerLevel = new LinkedList<>( rels );

        while ( !innerLevel.isEmpty() ) {
            RelNode relNode = innerLevel.pop();
            alreadyChecked.add( relNode );
            if ( relNode instanceof Project ) {
                return true;
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
        return false;
    }

}
