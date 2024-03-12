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

package org.polypheny.db.cypher.expression;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.cypher.admin.CypherWithGraph;
import org.polypheny.db.cypher.clause.CypherUseClause;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;

@Getter
@Slf4j
public class CypherUseGraph extends CypherWithGraph implements ExecutableStatement {

    private final CypherWithGraph statement;
    private final CypherUseClause useClause;


    public CypherUseGraph( CypherWithGraph statement, CypherUseClause useClause ) {
        super( ParserPos.ZERO );
        this.statement = statement;
        this.useClause = useClause;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        if ( this.statement != null && this.statement.isDdl() ) {
            ((ExecutableStatement) this.statement).execute( context, statement, parsedQueryContext );
        }
        if ( useClause != null ) {
            log.warn( useClause.toString() );
        }
    }


    @Override
    public boolean isDdl() {
        return true;
    }

}
