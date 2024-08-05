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

package org.polypheny.db.prisminterface.statements;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.prism.StatementResult;

@Getter
@Slf4j
public class PIUnparameterizedStatement extends PIStatement {

    private final String query;
    private Statement statement;
    @Setter
    private PolyImplementation implementation;


    public PIUnparameterizedStatement( int id, PIClient client, QueryLanguage language, LogicalNamespace namespace, String query ) {
        super(
                id,
                client,
                language,
                namespace
        );
        this.query = query;
    }


    public StatementResult execute( int fetchSize ) {
        statement = client.getOrCreateNewTransaction().createStatement();
        StatementProcessor.implement( this );
        return StatementProcessor.executeAndGetResult( this, fetchSize );
    }


    @Override
    public void close() {
        if ( statement != null ) {
            statement.close();
        }
        closeResults();
    }


    @Override
    public Transaction getTransaction() {
        return statement.getTransaction();
    }

}
