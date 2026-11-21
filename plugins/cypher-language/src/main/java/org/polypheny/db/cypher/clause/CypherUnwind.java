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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;


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
        if ( !context.hasInput() ) {
            // A leading UNWIND evaluates its expression against one empty input row.
            context.add( context.asValues( List.of() ) );
        }
        AlgNode input = context.peek();
        String alias = variable.getName();
        List<String> fields = input.getTupleType().getFieldNames();
        if ( fields.contains( alias ) ) {
            throw new CypherSyntaxException( "Variable is already bound in UNWIND: " + alias );
        }

        RexNode values = expression.getRex( context, RexType.PROJECT ).right;
        List<RexNode> projections = new ArrayList<>();
        List<PolyString> names = new ArrayList<>();
        for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
            // An anonymous match contributes rows but has no variable to carry into scope.
            if ( field.getName() != null ) {
                projections.add( context.rexBuilder.makeInputRef( input, field.getIndex() ) );
                names.add( PolyString.of( field.getName() ) );
            }
        }

        String temporaryName = "$unwind$list";
        while ( fields.contains( temporaryName ) || alias.equals( temporaryName ) ) {
            temporaryName += "_";
        }
        int unwindIndex = projections.size();
        projections.add( values );
        names.add( PolyString.of( temporaryName ) );

        AlgNode project = LogicalLpgProject.create( context.pop(), projections, names );
        context.add( LogicalLpgUnwind.create( project, unwindIndex, alias ) );
    }

}
