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

package org.polypheny.db.adapter.file.rel;


import com.google.gson.Gson;
import java.util.ArrayList;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;


public class FileFilter extends Filter implements FileRel {

    protected FileFilter( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition ) {
        super( cluster, traits, child, condition );
    }

    @Override
    public Filter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return new FileFilter( getCluster(), traitSet, input, condition );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }

    @Override
    public void implement( FileImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        Condition condition = new Condition( (RexCall) this.condition );//projectionMapping is not available yet
        implementor.setCondition( condition );
    }

    public static class Condition {

        private final SqlKind operator;
        private Integer columnReference;
        private Long literalIndex;
        private Object literal;
        private ArrayList<Condition> operands = new ArrayList<>();

        public Condition( final RexCall call ) {
            this.operator = call.getOperator().getKind();
            for ( RexNode rex : call.getOperands() ) {
                if ( rex instanceof RexCall ) {
                    this.operands.add( new Condition( (RexCall) rex ) );
                } else {
                    RexNode n0 = call.getOperands().get( 0 );
                    assignRexNode( n0 );
                    if ( call.getOperands().size() > 1 ) { // IS NULL and IS NOT NULL have no literal/literalIndex
                        RexNode n1 = call.getOperands().get( 1 );
                        assignRexNode( n1 );
                    }
                }
            }
        }

        private void assignRexNode( final RexNode rexNode ) {
            if ( rexNode instanceof RexInputRef ) {
                this.columnReference = ((RexInputRef) rexNode).getIndex();
            } else if ( rexNode instanceof RexDynamicParam ) {
                this.literalIndex = ((RexDynamicParam) rexNode).getIndex();
            } else if ( rexNode instanceof RexLiteral ) {
                this.literal = ((RexLiteral) rexNode).getValueForQueryParameterizer();//todo find best getValue method
            }
        }

        public boolean matches( final Comparable[] columnValues, final DataContext dataContext ) {
            if ( columnReference == null ) { // || literalIndex == null ) {
                switch ( operator ) {
                    case AND:
                        for ( Condition c : operands ) {
                            if ( !c.matches( columnValues, dataContext ) ) {
                                return false;
                            }
                        }
                        return true;
                    case OR:
                        for ( Condition c : operands ) {
                            if ( c.matches( columnValues, dataContext ) ) {
                                return true;
                            }
                        }
                        return false;
                    default:
                        throw new RuntimeException( operator + " not supported in condition without columnReference" );
                }
            }
            //TODO find better names for parameterValue and cmp
            Object parameterValue;
            if ( this.literal != null ) {
                parameterValue = this.literal;
            } else {
                parameterValue = dataContext.getParameterValue( literalIndex );
            }
            Comparable cmp = columnValues[columnReference];//don't do the projectionMapping here
            if ( cmp == null ) {
                switch ( operator ) {
                    case IS_NULL:
                        return true;
                    case IS_NOT_NULL:
                    default:// null > 3 returns false
                        return false;
                }
            }
            if ( cmp instanceof Number && parameterValue instanceof Number ) {
                cmp = ((Number) cmp).doubleValue();
                parameterValue = ((Number) parameterValue).doubleValue();
            }

            int comparison = cmp.compareTo( parameterValue );
            switch ( operator ) {
                case AND:
                    for ( Condition c : operands ) {
                        if ( !c.matches( columnValues, dataContext ) ) {
                            return false;
                        }
                    }
                    return true;
                case OR:
                    for ( Condition c : operands ) {
                        if ( c.matches( columnValues, dataContext ) ) {
                            return true;
                        }
                    }
                    return false;
                case EQUALS:
                    return comparison == 0;
                case GREATER_THAN:
                    return comparison > 0;
                case GREATER_THAN_OR_EQUAL:
                    return comparison >= 0;
                case LESS_THAN:
                    return comparison < 0;
                case LESS_THAN_OR_EQUAL:
                    return comparison <= 0;
                default:
                    throw new RuntimeException( operator + " comparison not supported by file adapter." );
            }
        }

        public String toJson() {
            return new Gson().toJson( this, Condition.class );
        }

        public static Condition fromJson( final String serlializedCondition ) {
            return new Gson().fromJson( serlializedCondition, Condition.class );
        }
    }
}
