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

package org.polypheny.db.adapter.cottontail.algebra;


import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.cottontail.CottontailEntity;
import org.polypheny.db.adapter.cottontail.algebra.CottontailAlg.CottontailImplementContext.QueryType;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;


public class CottontailTableModify extends RelModify<CottontailEntity> implements CottontailAlg {

    public final CottontailEntity cottontailTable;


    /**
     * Creates a {@code Modify}.
     * <p>
     * The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param traitSet Traits of this relational expression
     * @param table Target table to modify
     * @param input Sub-query or filter condition
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    public CottontailTableModify(
            AlgTraitSet traitSet,
            CottontailEntity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList,
            boolean flattened ) {
        super( input.getCluster(), traitSet, table, input, operation, updateColumnList, sourceExpressionList, flattened );
        this.cottontailTable = table;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new CottontailTableModify(
                traitSet,
                entity,
                AbstractAlgNode.sole( inputs ),
                getOperation(),
                getUpdateColumns(),
                getSourceExpressions(),
                isFlattened() );
    }


    @Override
    public void register( AlgPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        context.table = this.entity;
        context.schemaName = this.cottontailTable.getPhysicalSchemaName();
        context.tableName = this.cottontailTable.getPhysicalTableName();
        context.visitChild( 0, getInput() );

        switch ( this.getOperation() ) {
            case INSERT:
                context.queryType = QueryType.INSERT;
                break;
            case UPDATE:
                context.queryType = QueryType.UPDATE;
                context.preparedValuesMapBuilder = buildUpdateTupleBuilder( context );
                break;
            case DELETE:
                context.queryType = QueryType.DELETE;
                break;
            case MERGE:
                throw new GenericRuntimeException( "Merge is not supported." );
        }

    }


    private Expression buildUpdateTupleBuilder( CottontailImplementContext context ) {
        BlockBuilder inner = new BlockBuilder();

        final List<String> physicalColumnNames = new ArrayList<>();
        final List<String> logicalColumnNames = new ArrayList<>();
        final List<PolyType> columnTypes = new ArrayList<>();
        for ( AlgDataTypeField field : context.table.getTupleType().getFields() ) {
            physicalColumnNames.add( field.getPhysicalName() );
            logicalColumnNames.add( field.getName() );
            columnTypes.add( field.getType().getPolyType() );
        }

        ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, inner.newName( "dynamicParameters" ) );

        ParameterExpression valuesMap_ = Expressions.variable( Map.class, inner.newName( "valuesMap" ) );
        NewExpression valuesMapCreator_ = Expressions.new_( LinkedHashMap.class );
        inner.add( Expressions.declare( Modifier.FINAL, valuesMap_, valuesMapCreator_ ) );


        for ( int i = 0; i < getSourceExpressions().size(); i++ ) {
            RexNode rexNode = getSourceExpressions().get( i );
            final String logicalName = getUpdateColumns().get( i );
            final int actualColumnIndex = logicalColumnNames.indexOf( logicalName );
            final String originalName = physicalColumnNames.get( actualColumnIndex );

            Expression source_;
            if ( rexNode instanceof RexLiteral ) {
                source_ = CottontailTypeUtil.rexLiteralToDataExpression( (RexLiteral) rexNode, columnTypes.get( actualColumnIndex ) );
            } else if ( rexNode instanceof RexDynamicParam ) {
                source_ = CottontailTypeUtil.rexDynamicParamToDataExpression( (RexDynamicParam) rexNode, dynamicParameterMap_, columnTypes.get( actualColumnIndex ) );
            } else if ( (rexNode instanceof RexCall) && (((RexCall) rexNode).getOperator() instanceof SqlArrayValueConstructor) ) {
                source_ = CottontailTypeUtil.rexArrayConstructorToExpression( (RexCall) rexNode, columnTypes.get( actualColumnIndex ) );
            } else {
                throw new GenericRuntimeException( "unable to convert expression." );
            }

            inner.add( Expressions.statement(
                    Expressions.call(
                            valuesMap_,
                            BuiltInMethod.MAP_PUT.method,
                            Expressions.constant( originalName ),
                            source_ ) ) );
        }

        inner.add( Expressions.return_( null, valuesMap_ ) );

        return Expressions.lambda( inner.toBlock(), dynamicParameterMap_ );
    }

}
