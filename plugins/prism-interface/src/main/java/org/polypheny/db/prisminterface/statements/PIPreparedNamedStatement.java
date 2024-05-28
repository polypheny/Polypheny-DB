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

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.NamedValueProcessor;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.StatementResult;

public class PIPreparedNamedStatement extends PIPreparedStatement {

    @Getter
    private final String query;
    @Getter
    @Setter
    private PolyImplementation implementation;
    @Getter
    private Statement statement;
    private final NamedValueProcessor namedValueProcessor;


    public PIPreparedNamedStatement(
            int id,
            PIClient client,
            QueryLanguage language,
            LogicalNamespace namespace,
            String query ) {
        super(
                id, client, language, namespace
        );
        this.namedValueProcessor = new NamedValueProcessor( query );
        this.query = namedValueProcessor.getProcessedQuery();
    }


    @SuppressWarnings("Duplicates")
    public StatementResult execute( Map<String, PolyValue> values, int fetchSize ) throws Exception {
        if ( statement == null || client.hasNoTransaction() ) {
            statement = client.getOrCreateNewTransaction().createStatement();
        } else {
            statement.getDataContext().resetParameterValues();
        }
        List<PolyValue> valueList = namedValueProcessor.transformValueMap( values );
        for ( int i = 0; i < valueList.size(); i++ ) {
            statement.getDataContext().addParameterValues( i, PolyValue.deriveType( valueList.get( i ), this.statement.getDataContext().getTypeFactory() ), List.of( valueList.get( i ) ) );
        }
        StatementProcessor.implement( this );
        return StatementProcessor.executeAndGetResult( this, fetchSize );
    }


    @Override
    public void close() {
        statement.close();
        closeResults();
    }


    @Override
    public Transaction getTransaction() {
        return statement.getTransaction();
    }

}
