/*
 * Copyright 2019-2023 The Polypheny Project
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
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.cypher.CypherCallResultItem;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.type.entity.PolyValue;


@Getter
public class CypherCall extends CypherClause {

    private final ParserPos nextPos;
    private final ParserPos procedurePos;
    private final ParserPos resultPos;
    private final List<String> namespace;
    private final String name;
    private final List<CypherExpression> arguments;
    @Nullable
    private final List<PolyValue> implicitArguments;
    private final boolean yieldAll;
    private final List<CypherCallResultItem> items;
    @Nullable
    private final CypherWhere where;


    public CypherCall(
            ParserPos pos,
            ParserPos nextPos,
            ParserPos procedurePos,
            ParserPos resultPos,
            List<String> namespace,
            String name,
            List<CypherExpression> arguments,
            boolean yieldAll,
            List<CypherCallResultItem> items,
            CypherWhere where ) {
        this( pos, nextPos, procedurePos, resultPos, namespace, name, arguments, null, yieldAll, items, where );
    }


    private CypherCall( ParserPos pos, ParserPos nextPos, ParserPos procedurePos, ParserPos resultPos,
            List<String> namespace, String name, List<CypherExpression> arguments, @Nullable List<PolyValue> implicitArguments,
            boolean yieldAll, List<CypherCallResultItem> items, CypherWhere where ) {
        super( pos );
        this.nextPos = nextPos;
        this.procedurePos = procedurePos;
        this.resultPos = resultPos;
        this.namespace = namespace;
        this.name = name;
        this.arguments = arguments;
        this.implicitArguments = implicitArguments;
        this.yieldAll = yieldAll;
        this.items = items;
        this.where = where;
    }


    public CypherCall withImplicitArguments( List<PolyValue> values ) {
        return new CypherCall( getPos(), nextPos, procedurePos, resultPos, namespace, name, null, List.copyOf( values ), yieldAll, items, where );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.CALL;
    }


    @Override
    public List<? extends Node> getInputs() {
        List<Node> inputs = new ArrayList<>();
        if ( arguments != null ) {
            inputs.addAll( arguments );
        }
        if ( where != null ) {
            inputs.add( where.getExpression() );
        }
        return inputs;
    }

}
