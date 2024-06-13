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

package org.polypheny.db.prepare;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;

/**
 * Basic translator.
 */
class EmptyScalarTranslator implements ScalarTranslator {

    private final RexBuilder rexBuilder;


    EmptyScalarTranslator( RexBuilder rexBuilder ) {
        this.rexBuilder = rexBuilder;
    }


    public static ScalarTranslator empty( RexBuilder builder ) {
        return new EmptyScalarTranslator( builder );
    }


    @Override
    public List<RexNode> toRexList( BlockStatement statement ) {
        final List<Expression> simpleList = simpleList( statement );
        final List<RexNode> list = new ArrayList<>();
        for ( Expression expression1 : simpleList ) {
            list.add( toRex( expression1 ) );
        }
        return list;
    }


    @Override
    public RexNode toRex( BlockStatement statement ) {
        return toRex( Blocks.simple( statement ) );
    }


    private static List<Expression> simpleList( BlockStatement statement ) {
        Expression simple = Blocks.simple( statement );
        if ( simple instanceof NewExpression newExpression ) {
            return newExpression.arguments;
        } else {
            return Collections.singletonList( simple );
        }
    }


    @Override
    public RexNode toRex( Expression expression ) {
        switch ( expression.getNodeType() ) {
            case MemberAccess:
                // Case-sensitive name match because name was previously resolved.
                return rexBuilder.makeFieldAccess(
                        toRex( ((MemberExpression) expression).expression ),
                        ((MemberExpression) expression).field.getName(),
                        true );
            case GreaterThan:
                return binary( expression, OperatorRegistry.get( OperatorName.GREATER_THAN, BinaryOperator.class ) );
            case LessThan:
                return binary( expression, OperatorRegistry.get( OperatorName.LESS_THAN, BinaryOperator.class ) );
            case Parameter:
                return parameter( (ParameterExpression) expression );
            case Call:
                MethodCallExpression call = (MethodCallExpression) expression;
                Operator operator = RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get( call.method );
                if ( operator != null ) {
                    return rexBuilder.makeCall(
                            type( call ),
                            operator,
                            toRex( Expressions.<Expression>list()
                                    .appendIfNotNull( call.targetExpression )
                                    .appendAll( call.expressions ) ) );
                }
                throw new RuntimeException( "Could translate call to method " + call.method );
            case Constant:
                final ConstantExpression constant = (ConstantExpression) expression;
                Object value = constant.value;
                if ( value instanceof Number number ) {
                    if ( value instanceof Double || value instanceof Float ) {
                        return rexBuilder.makeApproxLiteral( BigDecimal.valueOf( number.doubleValue() ) );
                    } else if ( value instanceof BigDecimal ) {
                        return rexBuilder.makeExactLiteral( (BigDecimal) value );
                    } else {
                        return rexBuilder.makeExactLiteral( BigDecimal.valueOf( number.longValue() ) );
                    }
                } else if ( value instanceof Boolean ) {
                    return rexBuilder.makeLiteral( (Boolean) value );
                } else {
                    return rexBuilder.makeLiteral( constant.toString() );
                }
            default:
                throw new UnsupportedOperationException( "unknown expression type " + expression.getNodeType() + " " + expression );
        }
    }


    private RexNode binary( Expression expression, BinaryOperator op ) {
        BinaryExpression call = (BinaryExpression) expression;
        return rexBuilder.makeCall( type( call ), op, toRex( ImmutableList.of( call.expression0, call.expression1 ) ) );
    }


    private List<RexNode> toRex( List<Expression> expressions ) {
        final List<RexNode> list = new ArrayList<>();
        for ( Expression expression : expressions ) {
            list.add( toRex( expression ) );
        }
        return list;
    }


    protected AlgDataType type( Expression expression ) {
        final Type type = expression.getType();
        return ((JavaTypeFactory) rexBuilder.getTypeFactory()).createType( type );
    }


    @Override
    public ScalarTranslator bind( List<ParameterExpression> parameterList, List<RexNode> values ) {
        return new LambdaScalarTranslator( rexBuilder, parameterList, values );
    }


    public RexNode parameter( ParameterExpression param ) {
        throw new RuntimeException( "unknown parameter " + param );
    }

}
