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

package org.polypheny.db.cypher.clause;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherExpression.ExpressionType;
import org.polypheny.db.cypher.expression.CypherLiteral;
import org.polypheny.db.cypher.expression.CypherLiteral.Literal;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.ComparableList;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;


@Getter
public class CypherUnwind extends CypherClause {

    private final CypherExpression expression;
    private final CypherVariable variable;


    public CypherUnwind( ParserPos pos, CypherExpression expression, CypherVariable variable ) {
        super( pos );
        this.expression = expression;
        this.variable = variable;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.UNWIND;
    }


    public void getUnwind( CypherContext context ) {
        Pair<PolyString, RexNode> namedNode;
        if ( expression.getType() == ExpressionType.LITERAL && ((CypherLiteral) expression).getLiteralType() == Literal.NULL ) {
            // special case, this is equal to empty list
            AlgDataType type = context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.ANY ), -1 );
            AlgDataType rowType = new AlgRecordType( List.of( new AlgDataTypeFieldImpl( -1L, variable.getName(), 0, type ) ) );

            RexLiteral emptyList = (RexLiteral) context.rexBuilder.makeLiteral( ComparableList.of(), type, false );

            ImmutableList<ImmutableList<RexLiteral>> values = ImmutableList.of( ImmutableList.of( emptyList ) );
            context.add( LogicalLpgValues.create( context.cluster, context.cluster.traitSet(), rowType, values ) );

            namedNode = Pair.of( PolyString.of( variable.getName() ), context.rexBuilder.makeInputRef( context.typeFactory.createPolyType( PolyType.ANY ), 0 ) );

        } else if ( expression.getType() == ExpressionType.LITERAL ) {
            // is values
            namedNode = expression.getValues( context, variable.getName() );
        } else {
            namedNode = expression.getRex( context, RexType.PROJECT );
        }

        AlgNode node = context.peek();

        if ( node.getTupleType().getFields().size() != 1 ) {
            if ( !node.getTupleType().getFieldNames().contains( namedNode.left ) ) {
                throw new UnsupportedOperationException();
            }
            node = new LogicalLpgProject(
                    node.getCluster(),
                    node.getTraitSet(),
                    context.pop(),
                    List.of( namedNode.right ),
                    List.of( namedNode.left ) );

            context.add( node );
        }

        if ( namedNode.left != null ) {
            // first project the expression out
            LogicalLpgProject llp = new LogicalLpgProject(
                    node.getCluster(),
                    node.getTraitSet(),
                    context.pop(),
                    List.of( namedNode.right ),
                    List.of( namedNode.left ) );
            context.add( llp );
        }

        context.add( new LogicalLpgUnwind(
                node.getCluster(),
                node.getTraitSet(),
                context.pop(),
                0,
                variable.getName() ) );
    }

}
