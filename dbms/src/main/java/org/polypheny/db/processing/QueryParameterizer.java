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

package org.polypheny.db.processing;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.polypheny.db.adapter.DataContext.ParameterValue;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.type.RelDataType;
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
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.PolyType;

public class QueryParameterizer extends RelShuttleImpl implements RexVisitor<RexNode> {

    private final AtomicInteger index;
    @Getter
    private final List<ParameterValue> values;
    @Getter
    private final List<RelDataType> types;


    public QueryParameterizer( int indexStart, List<RelDataType> parameterRowType ) {
        index = new AtomicInteger( indexStart );
        values = new ArrayList<>();
        types = new ArrayList<>( parameterRowType );
    }


    @Override
    public RelNode visit( LogicalFilter oFilter ) {
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
    public RelNode visit( LogicalProject oProject ) {
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
        int i = index.getAndIncrement();
        values.add( new ParameterValue( i, literal.getType(), literal.getValueForQueryParameterizer() ) );
        types.add( literal.getType() );
        return new RexDynamicParam( literal.getType(), i );
    }


    @Override
    public RexNode visitCall( RexCall call ) {
        if ( call.op instanceof SqlArrayValueConstructor ) {
            int i = index.getAndIncrement();
            List<Object> list = createListForArrays( call.operands );
            values.add( new ParameterValue( i, call.type, list ) );
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
        return subQuery.clone( subQuery.type, newOperands, subQuery.rel.accept( this ) );
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
