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

import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.statementProcessing.StatementProcessor;
import org.polypheny.db.protointerface.utils.PropertyUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;

public class PIPreparedIndexedStatement extends PIPreparedStatement {

    @Getter
    protected String query;
    @Getter
    protected Statement statement;
    @Getter
    @Setter
    protected PolyImplementation<PolyValue> implementation;


    public PIPreparedIndexedStatement(
            int id,
            PIClient client,
            QueryLanguage language,
            LogicalNamespace namespace,
            String query ) {
        super(
                id, client, language, namespace
        );
        this.query = query;
    }


    public List<Long> executeBatch( List<List<PolyValue>> valuesBatch) throws Exception {
        List<Long> updateCounts = new LinkedList<>();
        for ( List<PolyValue> values : valuesBatch ) {
            updateCounts.add( execute( values, PropertyUtils.DEFAULT_FETCH_SIZE ).getScalar() );
        }
        return updateCounts;
    }


    @SuppressWarnings("Duplicates")
    public StatementResult execute( List<PolyValue> values, int fetchSize ) throws Exception {
        synchronized ( client ) {
            if ( statement == null ) {
                statement = client.getCurrentOrCreateNewTransaction().createStatement();
            }
            long index = 0;
            for ( PolyValue value : values ) {
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
