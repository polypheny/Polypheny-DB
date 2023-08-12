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

import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.statements.PIStatement;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;

public class MongoExecutor extends StatementExecutor {

    private static QueryLanguage language = QueryLanguage.from( "mongo" );


    @Override
    public QueryLanguage getLanguage() {
        return language;
    }


    @Override
    public void execute( PIStatement piStatement ) throws PIServiceException {
        if ( hasInvalidLanguage( piStatement ) ) {
            throw new PIServiceException( "The statement in the language "
                    + piStatement.getLanguage()
                    + "can't be executed using a mql executor.",
                    "I9003",
                    9003
            );
        }
        Statement statement = piStatement.getStatement();
        if ( statement == null ) {
            throw new PIServiceException( "Statement is not linked to a polypheny statement",
                    "I9001",
                    9001
            );
        }
        String query = piStatement.getQuery();
        PolyImplementation<PolyValue> implementation;
        Processor queryProcessor = statement.getTransaction().getProcessor( language );
        //QueryParameters parameters = new MqlQueryParameters( query, null, NamespaceType.DOCUMENT );
        Node parsedStatement = queryProcessor.parse( query ).get( 0 );
        if ( parsedStatement.isA( Kind.DDL ) ) {
            implementation = queryProcessor.prepareDdl( statement, parsedStatement, null );
        } else {
            AlgRoot logicalRoot = queryProcessor.translate( statement, parsedStatement, null );
            implementation = statement.getQueryProcessor().prepareQuery( logicalRoot, true );
        }
        piStatement.setImplementation( implementation );
    }


}
