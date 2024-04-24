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

package org.polypheny.db.protointerface.statementProcessing;

import java.util.List;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.statements.PIStatement;
import org.polypheny.db.transaction.Statement;

public class SqlImplementer extends StatementImplementer {

    private static QueryLanguage language = QueryLanguage.from( "sql" );


    @Override
    public QueryLanguage getLanguage() {
        return language;
    }


    @Override
    public void implement( PIStatement piStatement ) throws PIServiceException {
        if ( hasInvalidLanguage( piStatement ) ) {
            throw new PIServiceException( "The statement in the language "
                    + piStatement.getLanguage()
                    + "can't be executed using a sql executor.",
                    "I9003",
                    9003
            );
        }
        Statement statement = piStatement.getStatement();
        if ( statement == null ) {
            throw new PIServiceException( "Statement is not linked to a polypheny statement",
                    "I9003",
                    9003
            );
        }
        QueryContext context = QueryContext.builder()
                .query( piStatement.getQuery() )
                .language( QueryLanguage.from( "sql" ) )
                .namespaceId( piStatement.getNamespace().id )
                .origin( ORIGIN )
                .build();
        List<ImplementationContext> implementations = LanguageManager.getINSTANCE().anyPrepareQuery( context, statement );
        piStatement.setImplementation( implementations.get( 0 ).getImplementation() );
    }

}
