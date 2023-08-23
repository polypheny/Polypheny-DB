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
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.statementProcessing.StatementProcessor;
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
        synchronized ( client ) {
            Transaction transaction;
            if ( statement == null ) {
                statement = client.getCurrentOrCreateNewTransaction().createStatement();
            } else {
                statement.getDataContext().resetParameterValues();
            }
            List<AlgDataType> types = valuesBatch.stream()
                    .map(v -> v.get(0).getType())
                    .map( v -> statement.getTransaction().getTypeFactory().createPolyType(v))
                    .collect(Collectors.toList());
            int i = 0;
            for ( List<PolyValue> column : valuesBatch ) {
                statement.getDataContext().addParameterValues( i, types.get( i++ ), column );
            }
            StatementProcessor.implement( this);
            updateCounts.add(StatementProcessor.executeAndGetResult(this, 0).getScalar());
        }
        return updateCounts;
    }


    @SuppressWarnings("Duplicates")
    public StatementResult execute( List<PolyValue> values, int fetchSize ) throws Exception {
        synchronized ( client ) {
            Transaction transaction;
            if ( statement == null ) {
                statement = client.getCurrentOrCreateNewTransaction().createStatement();
            } else {
                statement.getDataContext().resetParameterValues();
            }
            long index = 0;
            for ( PolyValue value : values ) {
                if ( value != null ) {
                    AlgDataType algDataType = statement.getTransaction().getTypeFactory().createPolyType(value.getType());
                    statement.getDataContext().addParameterValues( index++, algDataType, List.of( value ) );
                }
            }
            StatementProcessor.implement( this );
            return StatementProcessor.executeAndGetResult( this, fetchSize );
        }
    }

    @Override
    public Transaction getTransaction() {
        return statement.getTransaction();
    }
}
