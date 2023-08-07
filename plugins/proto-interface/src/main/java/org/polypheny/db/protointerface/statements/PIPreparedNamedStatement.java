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

package org.polypheny.db.protointerface.statements;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.NamedValueProcessor;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.statementProcessing.StatementProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;

public class PIPreparedNamedStatement extends PIPreparedStatement {

    @Getter
    protected String query;
    @Getter
    @Setter
    protected PolyImplementation<PolyValue> implementation;
    @Getter
    protected Statement statement;
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
        synchronized ( client ) {
            if ( statement == null ) {
                statement = client.getCurrentOrCreateNewTransaction().createStatement();
            }
            List<PolyValue> valueList = namedValueProcessor.transformValueMap( values );
            long index = 0;
            for ( PolyValue value : valueList ) {
                if ( value != null ) {
                    statement.getDataContext().addParameterValues( index++, null, List.of( value ) );
                }
            }
            StatementProcessor.execute( this );
            return StatementProcessor.getResult( this, fetchSize );
        }
    }


    @Override
    public Transaction getTransaction() {
        return statement.getTransaction();
    }

}
