/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.cypher.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.pattern.CypherShortestPathPattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

@Getter
public class CypherExpression extends CypherNode {

    private final ExpressionType type;
    private CypherVariable variable;
    private CypherExpression expression;
    private CypherExpression where;
    private CypherPattern pattern;


    public CypherExpression( ParserPos pos ) {
        super( pos );
        this.type = ExpressionType.DEFAULT;
    }


    public CypherExpression( ParserPos pos, ExpressionType type, CypherVariable variable, CypherExpression expression, CypherExpression where ) {
        super( pos );
        this.type = type;
        this.variable = variable;
        this.expression = expression;
        this.where = where;
    }


    public CypherExpression( ParserPos pos, ExpressionType type, CypherPattern pattern ) {
        super( pos );
        this.type = type;
        this.pattern = pattern;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.EXPRESSION;
    }


    public final Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        RexNode projected = context.projectedExpression( this );
        return projected == null ? toRex( context, type ) : Pair.of( null, projected );
    }


    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        if ( this instanceof CypherFunctionInvocation func ) {
            // var is null in case of function call
            return Pair.of( PolyString.of( null ), func.getRexCall( context, type ) );
        }

        if ( pattern instanceof CypherShortestPathPattern shortest ) {
            return Pair.of( null, CypherShortestPathValue.toRex( shortest, context, type ) );
        }
        if ( this.type == ExpressionType.PATTERN && pattern instanceof CypherEveryPathPattern everyPath ) {
            if ( type != RexType.FILTER ) {
                throw new CypherSyntaxException( "Use exists(pattern) to evaluate a pattern as a value expression." );
            }
            return Pair.of( null, CypherPatternExistence.toRex( everyPath, context, type ) );
        }

        OperatorName operatorName = switch ( this.type ) {
            case ALL -> OperatorName.CYPHER_ALL_MATCH;
            case ANY -> OperatorName.CYPHER_ANY_MATCH;
            case NONE -> OperatorName.CYPHER_NONE_MATCH;
            case SINGLE -> OperatorName.CYPHER_SINGLE_MATCH;
            default -> throw new UnsupportedOperationException();
        };

        String quantifier = this.type.name().toLowerCase( Locale.ROOT );
        if ( where == null ) {
            throw new CypherSyntaxException( quantifier + "() requires a WHERE predicate." );
        }
        RexNode list = expression.getRex( context, RexType.PROJECT ).right;
        requireListOperand( list, quantifier + "() requires a list." );
        AlgDataType component = list.getType().getComponentType();
        if ( component == null ) {
            component = context.typeFactory.createPolyType( PolyType.ANY );
        }
        Pair<RexNameRef, RexNode> predicate = context.quantifiedPredicate( variable.getName(), component, where );
        AlgDataType result = context.typeFactory.createTypeWithNullability( context.booleanType, true );
        return Pair.of( null, context.rexBuilder.makeCall( result, OperatorRegistry.get( QueryLanguage.from( "cypher" ), operatorName ),
                List.of( list, predicate.left, predicate.right ) ) );
    }


    static void requireListOperand( RexNode operand, String diagnostic ) {
        PolyType type = operand.getType().getPolyType();
        if ( type != PolyType.ARRAY && type != PolyType.ANY && type != PolyType.NULL
                && !(operand instanceof RexLiteral literal && literal.isNull()) ) {
            throw new CypherSyntaxException( diagnostic );
        }
    }


    public String getName() {
        throw new UnsupportedOperationException();
    }


    public Pair<PolyString, RexNode> getValues( CypherContext context, String name ) {
        Pair<PolyString, RexNode> namedNode = getRex( context, RexType.PROJECT );
        if ( namedNode.right.isA( Kind.LITERAL ) ) {
            ImmutableList<ImmutableList<RexLiteral>> values = ImmutableList.of( ImmutableList.of( (RexLiteral) namedNode.right ) );

            AlgRecordType rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( -1L, name, 0, namedNode.right.getType() ) ) );
            LogicalLpgValues node = LogicalLpgValues.create( context.cluster, context.cluster.traitSet(), rowType, values );

            context.add( node );
        } else {
            throw new UnsupportedOperationException();
        }

        return namedNode;
    }


    public PolyValue getComparable() {
        throw new UnsupportedOperationException();
    }


    public enum ExpressionType {
        ALL, NONE, SINGLE, PATTERN, ANY, VARIABLE, AGGREGATE, DEFAULT, LITERAL
    }

}
