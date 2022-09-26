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

package org.polypheny.db.processing.shuttles;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.bson.BsonValue;
import org.polypheny.db.adapter.DataContext.ParameterValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexRangeRef;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexTableInputRef;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.PolyType;

public class QueryParameterizer extends AlgShuttleImpl implements RexVisitor<RexNode> {

    private final AtomicInteger index;
    @Getter
    private final Map<Integer, List<ParameterValue>> values;

    @Getter
    private final Map<Integer, Map<Integer, List<ParameterValue>>> docs;

    @Getter
    private final List<AlgDataType> types;
    private final boolean asymmetric;
    private int batchSize;


    public QueryParameterizer( int indexStart, List<AlgDataType> parameterRowType, boolean asymmetric ) {
        index = new AtomicInteger( indexStart );
        values = new HashMap<>();
        types = new ArrayList<>( parameterRowType );
        this.asymmetric = asymmetric;
        docs = new HashMap<>();
    }


    @Override
    public AlgNode visit( LogicalFilter oFilter ) {
        if ( asymmetric ) {
            return oFilter;
        }

        LogicalFilter filter = (LogicalFilter) super.visit( oFilter );
        RexNode condition = filter.getCondition();
        return new LogicalFilter(
                filter.getCluster(),
                filter.getTraitSet(),
                filter.getInput(),
                condition.accept( this ),
                filter.getVariablesSet() );
    }


    @Override
    public AlgNode visit( LogicalLpgFilter oFilter ) {
        LogicalLpgFilter filter = (LogicalLpgFilter) super.visit( oFilter );
        RexNode condition = filter.getCondition();
        return new LogicalLpgFilter(
                filter.getCluster(),
                filter.getTraitSet(),
                filter.getInput(),
                condition );
    }


    @Override
    public AlgNode visit( LogicalDocumentFilter oFilter ) {
        LogicalDocumentFilter filter = (LogicalDocumentFilter) super.visit( oFilter );
        RexNode condition = filter.condition;
        return new LogicalDocumentFilter(
                filter.getCluster(),
                filter.getTraitSet(),
                filter.getInput(),
                condition.accept( this ) );
    }


    @Override
    public AlgNode visit( LogicalProject oProject ) {
        if ( asymmetric ) {
            return oProject;
        }
        LogicalProject project = (LogicalProject) super.visit( oProject );
        List<RexNode> newProjects = new ArrayList<>();
        for ( RexNode node : oProject.getProjects() ) {
            newProjects.add( node.accept( this ) );
        }
        return new LogicalProject(
                project.getCluster(),
                project.getTraitSet(),
                project.getInput(),
                newProjects,
                project.getRowType() );
    }


    @Override
    public AlgNode visit( LogicalLpgProject oProject ) {
        LogicalLpgProject project = (LogicalLpgProject) super.visit( oProject );
        /*List<RexNode> newProjects = new ArrayList<>();
        for ( RexNode node : oProject.getProjects() ) {
            newProjects.add( node.accept( this ) );
        }*/ //todo support parameterization for graph
        return new LogicalLpgProject(
                project.getCluster(),
                project.getTraitSet(),
                project.getInput(),
                project.getProjects(),
                project.getNames() );
    }


    @Override
    public AlgNode visit( LogicalDocumentProject oProject ) {
        LogicalDocumentProject project = (LogicalDocumentProject) super.visit( oProject );
        List<RexNode> newProjects = new ArrayList<>();
        for ( RexNode node : oProject.projects ) {
            newProjects.add( node.accept( this ) );
        }
        return new LogicalDocumentProject(
                project.getCluster(),
                project.getTraitSet(),
                project.getInput(),
                newProjects,
                project.getRowType() );
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute input ) {
        AlgNode right = input.getRight().accept( this );
        AlgNode left = input.getLeft();
        // left is always only one batch as it needs to return only one ResultSet
        // right can be more, so we can only parametrize left if right has the same size
        if ( this.batchSize == 1 ) {
            left = left.accept( this );
        }
        return new LogicalConditionalExecute(
                input.getCluster(),
                input.getTraitSet(),
                left,
                right,
                input.getCondition(),
                input.getExceptionClass(),
                input.getExceptionMessage() );
    }


    @Override
    public AlgNode visit( LogicalConstraintEnforcer enforcer ) {
        AlgNode modify = enforcer.getLeft().accept( this );

        return new LogicalConstraintEnforcer(
                enforcer.getCluster(),
                enforcer.getTraitSet(),
                modify,
                enforcer.getRight(),
                enforcer.getExceptionClasses(),
                enforcer.getExceptionMessages() );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        if ( other instanceof LogicalModifyCollect ) {
            if ( other.getTraitSet().contains( ModelTrait.GRAPH ) ) {
                //return other;
            }
            List<AlgNode> inputs = new ArrayList<>( other.getInputs().size() );
            for ( AlgNode node : other.getInputs() ) {
                inputs.add( node.accept( this ) );
            }
            return new LogicalModifyCollect(
                    other.getCluster(),
                    other.getTraitSet(),
                    inputs,
                    ((LogicalModifyCollect) other).all
            );
        } else {
            return super.visit( other );
        }
    }


    @Override
    public AlgNode visit( LogicalModify initial ) {
        if ( asymmetric ) {
            return visitAsymmetricModify( initial );
        }

        LogicalModify modify = (LogicalModify) super.visit( initial );
        List<RexNode> newSourceExpression = null;
        if ( modify.getSourceExpressionList() != null ) {
            newSourceExpression = new ArrayList<>();
            for ( RexNode node : modify.getSourceExpressionList() ) {
                newSourceExpression.add( node.accept( this ) );
            }
        }
        AlgNode input = modify.getInput();
        if ( input instanceof LogicalValues ) {
            List<RexNode> projects = new ArrayList<>();
            boolean firstRow = true;
            HashMap<Integer, Integer> idxMapping = new HashMap<>();
            this.batchSize = ((LogicalValues) input).tuples.size();
            for ( ImmutableList<RexLiteral> node : ((LogicalValues) input).getTuples() ) {
                int i = 0;
                for ( RexLiteral literal : node ) {
                    int idx;
                    if ( !idxMapping.containsKey( i ) ) {
                        idx = index.getAndIncrement();
                        idxMapping.put( i, idx );
                    } else {
                        idx = idxMapping.get( i );
                    }
                    AlgDataType type = input.getRowType().getFieldList().get( i ).getValue();
                    if ( firstRow ) {
                        projects.add( new RexDynamicParam( type, idx ) );
                    }
                    if ( !values.containsKey( idx ) ) {
                        types.add( type );
                        values.put( idx, new ArrayList<>( ((LogicalValues) input).getTuples().size() ) );
                    }
                    values.get( idx ).add( new ParameterValue( idx, type, literal.getValueForQueryParameterizer() ) );
                    i++;
                }
                firstRow = false;
            }
            LogicalValues logicalValues = LogicalValues.createOneRow( input.getCluster() );
            input = new LogicalProject(
                    input.getCluster(),
                    input.getTraitSet(),
                    logicalValues,
                    projects,
                    input.getRowType()
            );
        }
        return new LogicalModify(
                modify.getCluster(),
                modify.getTraitSet(),
                modify.getTable(),
                modify.getCatalogReader(),
                input,
                modify.getOperation(),
                modify.getUpdateColumnList(),
                newSourceExpression,
                modify.isFlattened() )
                .isStreamed( modify.isStreamed() );

    }


    public AlgNode visitAsymmetricModify( LogicalModify initial ) {
        LogicalModify modify = (LogicalModify) super.visit( initial );
        List<RexNode> newSourceExpression = null;
        if ( modify.getSourceExpressionList() != null ) {
            newSourceExpression = new ArrayList<>();
            for ( RexNode node : modify.getSourceExpressionList() ) {
                newSourceExpression.add( node.accept( this ) );
            }
        }
        AlgNode input = modify.getInput();
        if ( input instanceof LogicalValues ) {
            List<RexNode> projects = new ArrayList<>();
            boolean firstRow = true;
            HashMap<Integer, Integer> idxMapping = new HashMap<>();
            this.batchSize = ((LogicalValues) input).tuples.size();

            int entires = docs.size();
            HashMap<Integer, List<ParameterValue>> doc = new HashMap<>();

            for ( ImmutableList<RexLiteral> node : ((LogicalValues) input).getTuples() ) {
                int i = 0;
                for ( RexLiteral literal : node ) {
                    int idx;
                    if ( !idxMapping.containsKey( i ) ) {
                        idx = index.getAndIncrement();
                        idxMapping.put( i, idx );
                    } else {
                        idx = idxMapping.get( i );
                    }
                    AlgDataType type = input.getRowType().getFieldList().get( i ).getValue();
                    if ( firstRow ) {
                        projects.add( new RexDynamicParam( type, idx ) );
                    }
                    if ( !doc.containsKey( idx ) ) {
                        types.add( type );
                        doc.put( idx, new ArrayList<>( ((LogicalValues) input).getTuples().size() ) );
                    }
                    doc.get( idx ).add( new ParameterValue( idx, type, literal.getValueForQueryParameterizer() ) );
                    i++;
                }
                firstRow = false;
            }

            docs.put( entires, doc );

            LogicalValues logicalValues = LogicalValues.createOneRow( input.getCluster() );
            input = new LogicalProject(
                    input.getCluster(),
                    input.getTraitSet(),
                    logicalValues,
                    projects,
                    input.getRowType()
            );
        }
        return new LogicalModify(
                modify.getCluster(),
                modify.getTraitSet(),
                modify.getTable(),
                modify.getCatalogReader(),
                input,
                modify.getOperation(),
                modify.getUpdateColumnList(),
                newSourceExpression,
                modify.isFlattened() )
                .isStreamed( modify.isStreamed() );
    }


    @Override
    public AlgNode visit( LogicalDocumentModify initial ) {
        LogicalDocumentModify modify = (LogicalDocumentModify) super.visit( initial );
        List<RexNode> newSourceExpression = null;
        if ( modify.getUpdates() != null ) {
            newSourceExpression = new ArrayList<>();
            for ( RexNode node : modify.getUpdates() ) {
                newSourceExpression.add( node.accept( this ) );
            }
        }
        AlgNode input = modify.getInput();
        if ( input instanceof LogicalDocumentValues ) {
            List<RexNode> projects = new ArrayList<>();
            boolean firstRow = true;
            HashMap<Integer, Integer> idxMapping = new HashMap<>();
            this.batchSize = ((LogicalDocumentValues) input).documentTuples.size();
            for ( BsonValue node : ((LogicalDocumentValues) input).documentTuples ) {
                int i = 0;
                int idx;

                if ( !idxMapping.containsKey( i ) ) {
                    idx = index.getAndIncrement();
                    idxMapping.put( i, idx );
                } else {
                    idx = idxMapping.get( i );
                }

                AlgDataType type = input.getRowType().getFieldList().get( i ).getValue();
                if ( firstRow ) {
                    projects.add( new RexDynamicParam( type, idx ) );
                }
                if ( !values.containsKey( idx ) ) {
                    types.add( type );
                    values.put( idx, new ArrayList<>( ((LogicalDocumentValues) input).documentTuples.size() ) );
                }
                values.get( idx ).add( new ParameterValue( idx, type, node ) );

                firstRow = false;
            }
            LogicalDocumentValues logicalValues = LogicalDocumentValues.createOneRow( input.getCluster() );
            input = new LogicalDocumentProject(
                    input.getCluster(),
                    input.getTraitSet(),
                    logicalValues,
                    projects,
                    input.getRowType()
            );
        }
        return new LogicalDocumentModify(
                modify.getTraitSet(),
                modify.getCollection(),
                modify.getCatalogReader(),
                input,
                modify.operation,
                modify.getKeys(),
                newSourceExpression );

    }

    //
    // Rex
    //


    @Override
    public RexNode visitInputRef( RexInputRef inputRef ) {
        return inputRef;
    }


    @Override
    public RexNode visitLocalRef( RexLocalRef localRef ) {
        return localRef;
    }


    @Override
    public RexNode visitLiteral( RexLiteral literal ) {
        if ( literal.getType() instanceof IntervalPolyType && !RuntimeConfig.PARAMETERIZE_INTERVALS.getBoolean() ) {
            return literal;
        }
        int i = index.getAndIncrement();
        values.put( i, Collections.singletonList( new ParameterValue( i, literal.getType(), literal.getValueForQueryParameterizer() ) ) );
        types.add( literal.getType() );
        return new RexDynamicParam( literal.getType(), i );
    }


    @Override
    public RexNode visitCall( RexCall call ) {
        if ( call.getKind().belongsTo( Kind.MQL_KIND ) && call.op.getOperatorName() != OperatorName.MQL_QUERY_VALUE ) {
            return call;
        } else if ( call.op instanceof SqlArrayValueConstructor ) {
            int i = index.getAndIncrement();
            List<Object> list = createListForArrays( call.operands );
            values.put( i, Collections.singletonList( new ParameterValue( i, call.type, list ) ) );
            types.add( call.type );
            return new RexDynamicParam( call.type, i );
        } else {
            List<RexNode> newOperands = new LinkedList<>();
            for ( RexNode operand : call.operands ) {
                if ( operand instanceof RexLiteral && ((RexLiteral) operand).getTypeName() == PolyType.SYMBOL ) {
                    // Do not replace with dynamic param
                    newOperands.add( operand );
                } else {
                    newOperands.add( operand.accept( this ) );
                }
            }
            return new RexCall( call.type, call.op, newOperands );
        }
    }


    private List<Object> createListForArrays( List<RexNode> operands ) {
        List<Object> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                list.add( ((RexLiteral) node).getValueForQueryParameterizer() );
            } else if ( node instanceof RexCall ) {
                list.add( createListForArrays( ((RexCall) node).operands ) );
            } else {
                throw new RuntimeException( "Invalid array" );
            }
        }
        return list;
    }


    @Override
    public RexNode visitOver( RexOver over ) {
        List<RexNode> newOperands = new LinkedList<>();
        for ( RexNode operand : over.operands ) {
            newOperands.add( operand.accept( this ) );
        }
        return new RexCall( over.type, over.op, newOperands );
    }


    @Override
    public RexNode visitCorrelVariable( RexCorrelVariable correlVariable ) {
        return correlVariable;
    }


    @Override
    public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
        return dynamicParam;
    }


    @Override
    public RexNode visitRangeRef( RexRangeRef rangeRef ) {
        return rangeRef;
    }


    @Override
    public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
        return fieldAccess;
    }


    @Override
    public RexNode visitSubQuery( RexSubQuery subQuery ) {
        List<RexNode> newOperands = new LinkedList<>();
        for ( RexNode operand : subQuery.operands ) {
            newOperands.add( operand.accept( this ) );
        }
        return subQuery.clone( subQuery.type, newOperands, subQuery.alg.accept( this ) );
    }


    @Override
    public RexNode visitTableInputRef( RexTableInputRef fieldRef ) {
        return fieldRef;
    }


    @Override
    public RexNode visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
        return fieldRef;
    }

}
