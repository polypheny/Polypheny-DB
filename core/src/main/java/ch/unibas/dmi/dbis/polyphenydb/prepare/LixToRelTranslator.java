/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
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
 */

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable.ToRelContext;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable.ViewExpander;
import ch.unibas.dmi.dbis.polyphenydb.plan.ViewExpanders;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.Types;


/**
 * Translates a tree of linq4j {@link Queryable} nodes to a tree of {@link RelNode} planner nodes.
 *
 * @see QueryableRelBuilder
 */
class LixToRelTranslator {

    final RelOptCluster cluster;
    private final Prepare preparingStmt;
    final JavaTypeFactory typeFactory;


    LixToRelTranslator( RelOptCluster cluster, Prepare preparingStmt ) {
        this.cluster = cluster;
        this.preparingStmt = preparingStmt;
        this.typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
    }


    ToRelContext toRelContext() {
        if ( preparingStmt instanceof ViewExpander ) {
            final ViewExpander viewExpander = (ViewExpander) this.preparingStmt;
            return ViewExpanders.toRelContext( viewExpander, cluster );
        } else {
            return ViewExpanders.simpleContext( cluster );
        }
    }


    public <T> RelNode translate( Queryable<T> queryable ) {
        QueryableRelBuilder<T> translatorQueryable = new QueryableRelBuilder<>( this );
        return translatorQueryable.toRel( queryable );
    }


    public RelNode translate( Expression expression ) {
        if ( expression instanceof MethodCallExpression ) {
            final MethodCallExpression call = (MethodCallExpression) expression;
            BuiltInMethod method = BuiltInMethod.MAP.get( call.method );
            if ( method == null ) {
                throw new UnsupportedOperationException( "unknown method " + call.method );
            }
            RelNode input;
            switch ( method ) {
                case SELECT:
                    input = translate( call.targetExpression );
                    return LogicalProject.create(
                            input,
                            toRex( input, (FunctionExpression) call.expressions.get( 0 ) ),
                            (List<String>) null );

                case WHERE:
                    input = translate( call.targetExpression );
                    return LogicalFilter.create(
                            input,
                            toRex( (FunctionExpression) call.expressions.get( 0 ), input ) );

                case AS_QUERYABLE:
                    return LogicalTableScan.create(
                            cluster,
                            RelOptTableImpl.create(
                                    null,
                                    typeFactory.createJavaType( Types.toClass( Types.getElementType( call.targetExpression.getType() ) ) ),
                                    ImmutableList.of(),
                                    call.targetExpression ) );

                case SCHEMA_GET_TABLE:
                    return LogicalTableScan.create(
                            cluster,
                            RelOptTableImpl.create(
                                    null,
                                    typeFactory.createJavaType( (Class) ((ConstantExpression) call.expressions.get( 1 )).value ),
                                    ImmutableList.of(),
                                    call.targetExpression ) );

                default:
                    throw new UnsupportedOperationException( "unknown method " + call.method );
            }
        }
        throw new UnsupportedOperationException( "unknown expression type " + expression.getNodeType() );
    }


    private List<RexNode> toRex( RelNode child, FunctionExpression expression ) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> list = Collections.singletonList( rexBuilder.makeRangeReference( child ) );
        PolyphenyDbPrepareImpl.ScalarTranslator translator =
                PolyphenyDbPrepareImpl.EmptyScalarTranslator
                        .empty( rexBuilder )
                        .bind( expression.parameterList, list );
        final List<RexNode> rexList = new ArrayList<>();
        final Expression simple = Blocks.simple( expression.body );
        for ( Expression expression1 : fieldExpressions( simple ) ) {
            rexList.add( translator.toRex( expression1 ) );
        }
        return rexList;
    }


    List<Expression> fieldExpressions( Expression expression ) {
        if ( expression instanceof NewExpression ) {
            // Note: We are assuming that the arguments to the constructor are the same order as the fields of the class.
            return ((NewExpression) expression).arguments;
        }
        throw new RuntimeException( "unsupported expression type " + expression );
    }


    List<RexNode> toRexList( FunctionExpression expression, RelNode... inputs ) {
        List<RexNode> list = new ArrayList<>();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        for ( RelNode input : inputs ) {
            list.add( rexBuilder.makeRangeReference( input ) );
        }
        return PolyphenyDbPrepareImpl.EmptyScalarTranslator.empty( rexBuilder )
                .bind( expression.parameterList, list )
                .toRexList( expression.body );
    }


    RexNode toRex( FunctionExpression expression, RelNode... inputs ) {
        List<RexNode> list = new ArrayList<>();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        for ( RelNode input : inputs ) {
            list.add( rexBuilder.makeRangeReference( input ) );
        }
        return PolyphenyDbPrepareImpl.EmptyScalarTranslator.empty( rexBuilder )
                .bind( expression.parameterList, list )
                .toRex( expression.body );
    }
}
