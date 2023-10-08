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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.statements.PIStatement;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

public class SqlImplementer extends StatementImplementer {
    private static QueryLanguage language = QueryLanguage.from( "sql" );

    @Override
    public QueryLanguage getLanguage() {
        return language;
    }


    @Override
    public void implement(PIStatement piStatement ) throws PIServiceException{
        if ( hasInvalidLanguage( piStatement ) ) {
            throw new PIServiceException( "The statement in the language "
                    + piStatement.getLanguage()
                    + "can't be executed using a sql executor.",
                    "I9003",
                    9003
            );
        }
        Statement statement = piStatement.getStatement();
        if (statement == null) {
            throw new PIServiceException( "Statement is not linked to a polypheny statement",
                    "I9003",
                    9003
            );
        }
        String query = piStatement.getQuery();
        PolyImplementation implementation;
        Processor queryProcessor = statement.getTransaction().getProcessor( language );
        Node parsedStatement = queryProcessor.parse( query ).get( 0 );
        if ( parsedStatement.isA( Kind.DDL ) ) {
            implementation = queryProcessor.prepareDdl( statement, parsedStatement,
                    new QueryParameters( query, language.getNamespaceType() ) );
        } else {
            Pair<Node, AlgDataType> validated = queryProcessor.validate( piStatement.getTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = queryProcessor.translate( statement, validated.left, null );
            AlgDataType parameterRowType = queryProcessor.getParameterRowType( validated.left );
            implementation = statement.getQueryProcessor().prepareQuery( logicalRoot, parameterRowType, true );
        }
        piStatement.setImplementation( implementation );
    }
}
