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

import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.Lockable.LockType;


@Getter
public class CypherUseClause extends CypherClause implements ExecutableStatement {

    private final CypherExpression expression;


    public CypherUseClause( ParserPos pos, CypherExpression expression ) {
        super( pos );
        this.expression = expression;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.USE;
    }


    @Override
    public boolean isDdl() {
        return true;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {

    }


    @Override
    public Map<Lockable, LockType> deriveLockables( Context context, ParsedQueryContext parsedQueryContext ) {
        return Map.of();
    }


    @Override
    public Optional<String> switchesNamespace() {
        return Optional.ofNullable( expression.getName() );
    }

}
