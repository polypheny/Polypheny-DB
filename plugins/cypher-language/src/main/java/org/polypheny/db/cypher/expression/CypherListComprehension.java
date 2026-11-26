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

import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherListComprehension extends CypherExpression {

    private final CypherVariable variable;
    private final CypherExpression list;
    private final CypherExpression where;
    private final CypherExpression projection;


    public CypherListComprehension( ParserPos pos, CypherVariable variable, CypherExpression list, CypherExpression where, CypherExpression projection ) {
        super( pos );
        this.variable = variable;
        this.list = list;
        this.where = where;
        this.projection = projection;
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        RexNode input = list.getRex( context, RexType.PROJECT ).right;
        requireListOperand( input, "List comprehension requires a list." );
        AlgDataType component = input.getType().getComponentType();
        if ( component == null ) {
            component = context.typeFactory.createPolyType( PolyType.ANY );
        }
        RexNode result = context.listBinding( variable.getName(), component, element -> {
            RexNode predicate = where == null ? null : context.booleanPredicate( where.getRex( context, RexType.FILTER ).right );
            RexNode value = projection == null ? null : projection.getRex( context, RexType.PROJECT ).right;
            return project( context, input, element, predicate, value );
        } );
        return Pair.of( null, result );
    }


    static RexNode project( CypherContext context, RexNode list, RexNameRef element, @Nullable RexNode predicate, @Nullable RexNode projection ) {
        RexNode filter = predicate == null ? context.rexBuilder.makeLiteral( true ) : predicate;
        RexNode value = projection == null ? element : projection;
        AlgDataType arrayType = context.typeFactory.createArrayType( value.getType(), -1, 1 );
        return context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( arrayType, list.getType().isNullable() ),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_LIST_COMPREHENSION ),
                List.of( list, element, filter, value ) );
    }

}
