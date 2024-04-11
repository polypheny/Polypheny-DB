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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.prepare;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Translates a tree of linq4j {@link Queryable} nodes to a tree of {@link AlgNode} planner nodes.
 *
 * @see QueryableAlgBuilder
 */
class LixToAlgTranslator {

    final AlgCluster cluster;
    final JavaTypeFactory typeFactory;


    LixToAlgTranslator( AlgCluster cluster ) {
        this.cluster = cluster;
        this.typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
    }




    public AlgNode translate( Expression expression ) {
        if ( expression instanceof MethodCallExpression ) {
            final MethodCallExpression call = (MethodCallExpression) expression;
            BuiltInMethod method = BuiltInMethod.MAP.get( call.method );
            if ( method == null ) {
                throw new UnsupportedOperationException( "unknown method " + call.method );
            }
            AlgNode input;
            return switch ( method ) {
                case SELECT -> {
                    input = translate( call.targetExpression );
                    yield LogicalRelProject.create(
                            input,
                            toRex( input, (FunctionExpression<?>) call.expressions.get( 0 ) ),
                            (List<String>) null );
                }
                case WHERE -> {
                    input = translate( call.targetExpression );
                    yield LogicalRelFilter.create(
                            input,
                            toRex( (FunctionExpression<?>) call.expressions.get( 0 ), input ) );
                }
                default -> throw new UnsupportedOperationException( "unknown method " + call.method );
            };
        }
        throw new UnsupportedOperationException( "unknown expression type " + expression.getNodeType() );
    }


    private List<RexNode> toRex( AlgNode child, FunctionExpression<?> expression ) {
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


    List<RexNode> toRexList( FunctionExpression expression, AlgNode... inputs ) {
        List<RexNode> list = new ArrayList<>();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        for ( AlgNode input : inputs ) {
            list.add( rexBuilder.makeRangeReference( input ) );
        }
        return PolyphenyDbPrepareImpl.EmptyScalarTranslator.empty( rexBuilder )
                .bind( expression.parameterList, list )
                .toRexList( expression.body );
    }


    RexNode toRex( FunctionExpression expression, AlgNode... inputs ) {
        List<RexNode> list = new ArrayList<>();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        for ( AlgNode input : inputs ) {
            list.add( rexBuilder.makeRangeReference( input ) );
        }
        return PolyphenyDbPrepareImpl.EmptyScalarTranslator.empty( rexBuilder )
                .bind( expression.parameterList, list )
                .toRex( expression.body );
    }

}
