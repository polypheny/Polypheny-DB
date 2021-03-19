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

package org.polypheny.db.adapter.cottontail.rel;


import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.fun.SqlDistanceFunction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


public class CottontailSortAndProject extends SortAndProject implements CottontailRel {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param child Input relational expression
     */
    public CottontailSortAndProject( RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch,
            List<? extends RexNode> projects,
            RelDataType rowType,
            RelDataType projectRowType,
            boolean arrayProject ) {
        super( cluster, traits, child, collation, offset, fetch, projects, rowType, projectRowType, arrayProject );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        BlockBuilder builder = context.blockBuilder;
        context.visitChild( 0, getInput() );

        final List<String> physicalColumnNames = new ArrayList<>();
        final List<PolyType> columnTypes = new ArrayList<>();
        for ( RelDataTypeField field : context.cottontailTable.getRowType( getCluster().getTypeFactory() ).getFieldList() ) {
            physicalColumnNames.add( context.cottontailTable.getPhysicalColumnName( field.getName() ) );
            columnTypes.add( field.getType().getPolyType() );
        }

        if ( this.offset != null || this.fetch != null ) {

            if ( this.fetch != null ) {
                context.limitBuilder = numberBuilderBuilder( this.fetch );
            }

            if ( this.offset != null ) {
                context.offsetBuilder = numberBuilderBuilder( this.offset );
            }
        }

        if ( this.arrayProject ) {
//            BlockBuilder inner = new BlockBuilder();

            /*ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, inner.newName( "dynamicParameters" ) );

            ParameterExpression valuesMap_ = Expressions.variable( Map.class, inner.newName( "valuesMap" ) );
            NewExpression valuesMapCreator_ = Expressions.new_( HashMap.class );
            inner.add( Expressions.declare( Modifier.FINAL, valuesMap_, valuesMapCreator_ ) );

            List<Pair<RexNode, String>> namedProjects = getNamedProjects();

            for ( int i = 0; i < namedProjects.size(); i++ ) {
                Pair<RexNode, String> pair = namedProjects.get( i );
                final String originalName = physicalColumnNames.get( i );

                Expression source_;
                if ( pair.left instanceof RexLiteral ) {
                    source_ = CottontailTypeUtil.rexLiteralToDataExpression( (RexLiteral) pair.left );
                } else if ( pair.left instanceof RexDynamicParam ) {
                    source_ = CottontailTypeUtil.rexDynamicParamToDataExpression( (RexDynamicParam) pair.left, dynamicParameterMap_ );
                } else if ( (pair.left instanceof RexCall) && (((RexCall) pair.left).getOperator() instanceof SqlArrayValueConstructor) ) {
                    source_ = CottontailTypeUtil.rexArrayConstructorToExpression( (RexCall) pair.left );
                } else if ( pair.left instanceof RexInputRef ) {

                } else {
                    throw new RuntimeException( "unable to convert expression." );
                }

                inner.add( Expressions.statement(
                        Expressions.call( valuesMap_,
                                BuiltInMethod.MAP_PUT.method,
                                Expressions.constant( originalName ),
                                source_ ) ) );
            }

            inner.add( Expressions.return_( null, valuesMap_ ) );

            context.preparedValuesMapBuilder = Expressions.lambda( inner.toBlock(), dynamicParameterMap_ );*/
            context.preparedValuesMapBuilder = CottontailProject.makeProjectValueBuilder( context.blockBuilder, getNamedProjects(), physicalColumnNames, columnTypes );
            Pair<ParameterExpression, Expression> projectsAndKnn = makeProjectionAndKnnBuilder( context.blockBuilder, getNamedProjects(), physicalColumnNames, this.fetch );
            context.projectionMap = projectsAndKnn.left;
            if ( projectsAndKnn.right != null ) {
                context.knnBuilder = projectsAndKnn.right;
            }
        } else {
//            context.visitChild( 0, getInput() );
            /*final List<String> physicalColumnNames = new ArrayList<>();
            for ( RelDataTypeField field : context.cottontailTable.getRowType( getCluster().getTypeFactory() ).getFieldList() ) {
                physicalColumnNames.add( context.cottontailTable.getPhysicalColumnName( field.getName() ) );
            }*/

            /*final ParameterExpression projectionMap_ = Expressions.variable( Map.class, builder.newName( "projectionMap" ) );
            final NewExpression projectionMapCreator = Expressions.new_( HashMap.class );
            builder.add( Expressions.declare( Modifier.FINAL, projectionMap_, projectionMapCreator ) );


            for ( Pair<RexNode, String> pair : getNamedProjects() ) {
                if ( pair.left instanceof RexInputRef ) {
                    final String name = pair.right;
                    final String physicalName = physicalColumnNames.get( ((RexInputRef) pair.left).getIndex() );

                    builder.add( Expressions.statement(
                            Expressions.call( projectionMap_,
                                    BuiltInMethod.MAP_PUT.method,
                                    Expressions.constant( physicalName ),
                                    Expressions.constant( name.toLowerCase() ) ) ) );
                } else if ( pair.left instanceof RexCall ) {
                    // KNN Function pushdown
                    Expression knnBuilder = CottontailTypeUtil.knnCallToFunctionExpression( (RexCall) pair.left, physicalColumnNames );
                    context.knnBuilder = knnBuilder;

                    final String name = pair.right;
                    builder.add( Expressions.statement(
                            Expressions.call( projectionMap_,
                                    BuiltInMethod.MAP_PUT.method,
                                    Expressions.constant( "distance" ),
                                    Expressions.constant( name.toLowerCase() ) ) ) );
                }
            }*/

            context.blockBuilder = builder;
            Pair<ParameterExpression, Expression> projectsAndKnn = makeProjectionAndKnnBuilder( context.blockBuilder, getNamedProjects(), physicalColumnNames, this.fetch );
            context.projectionMap = projectsAndKnn.left;
            if ( projectsAndKnn.right != null ) {
                context.knnBuilder = projectsAndKnn.right;
            }
        }

    }


    @Override
    public SortAndProject copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelCollation newCollation, RexNode offset, RexNode fetch ) {
        return new CottontailSortAndProject( getCluster(), traitSet, input, collation, offset, fetch, projects, rowType, projectRowType, arrayProject );
    }


    public static Pair<ParameterExpression, Expression> makeProjectionAndKnnBuilder( BlockBuilder builder, List<Pair<RexNode, String>> namedProjects, List<String> physicalColumnNames, RexNode limitNode ) {
        final ParameterExpression projectionMap_ = Expressions.variable( Map.class, builder.newName( "projectionMap" ) );
        final NewExpression projectionMapCreator = Expressions.new_( HashMap.class );
        builder.add( Expressions.declare( Modifier.FINAL, projectionMap_, projectionMapCreator ) );

        Expression knnBuilder = null;

        for ( Pair<RexNode, String> pair : namedProjects ) {
            if ( pair.left instanceof RexInputRef ) {
                final String name = pair.right;
                final String physicalName = physicalColumnNames.get( ((RexInputRef) pair.left).getIndex() );

                builder.add( Expressions.statement(
                        Expressions.call( projectionMap_,
                                BuiltInMethod.MAP_PUT.method,
                                Expressions.constant( physicalName ),
                                Expressions.constant( name.toLowerCase() ) ) ) );
            } else if ( pair.left instanceof RexCall && (((RexCall) pair.left).getOperator() instanceof SqlDistanceFunction) ) {
                // KNN Function pushdown
                knnBuilder = CottontailTypeUtil.knnCallToFunctionExpression( (RexCall) pair.left, physicalColumnNames, limitNode );
//                context.knnBuilder = knnBuilder;

                final String name = pair.right;
                builder.add( Expressions.statement(
                        Expressions.call( projectionMap_,
                                BuiltInMethod.MAP_PUT.method,
                                Expressions.constant( "distance" ),
                                Expressions.constant( name.toLowerCase() ) ) ) );
            }
        }

        return new Pair<>( projectionMap_, knnBuilder );
    }


    private static Expression numberBuilderBuilder( RexNode node ) {
        BlockBuilder inner = new BlockBuilder();
        ParameterExpression dynamicParameterMap_ = Expressions.parameter( Modifier.FINAL, Map.class, inner.newName( "dynamicParameters" ) );

        Expression expr;
        if ( node instanceof RexLiteral ) {
            expr = Expressions.constant( ((RexLiteral) node).getValueAs( Integer.class ) );
        } else if ( node instanceof RexDynamicParam ) {
            expr = Expressions.call( dynamicParameterMap_, BuiltInMethod.MAP_GET.method, Expressions.constant( ((RexDynamicParam) node).getIndex() ) );
        } else {
            throw new RuntimeException( "Node statement is neither a Literal nor a Dynamic Parameter." );
        }

        inner.add( Expressions.return_( null, expr ) );

        return Expressions.lambda( inner.toBlock(), dynamicParameterMap_ );
    }

}
