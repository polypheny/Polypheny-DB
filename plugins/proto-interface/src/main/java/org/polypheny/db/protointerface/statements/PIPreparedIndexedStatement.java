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

import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class PIPreparedIndexedStatement extends PIPreparedStatement {
    String query;
    Statement statement;
    PolyImplementation<PolyValue> implementation;

    protected Statement currentStatement;



    protected PIPreparedIndexedStatement(Builder builder) {
        super(
                builder.id,
                builder.client,
                builder.properties,
                builder.language
        );
        this.query = builder.query;
    }

    public List<Long> executeBatch(List<List<PolyValue>> valuesBatch) throws Exception {
        List<Long> updateCounts = new LinkedList<>();
        for (List<PolyValue> values : valuesBatch) {
            updateCounts.add(execute(values).getScalar());
        }
        return updateCounts;
    }

    @SuppressWarnings("Duplicates")
    public StatementResult execute(List<PolyValue> values) throws Exception {
        synchronized (client) {
            if (currentStatement == null) {
                currentStatement = client.getCurrentOrCreateNewTransaction().createStatement();
            }
            long index = 0;
            for (PolyValue value : values) {
                if (value != null) {
                    currentStatement.getDataContext().addParameterValues(index++, null, List.of(value));
                }
            }
            StatementUtils.execute(this);
            StatementResult.Builder resultBuilder = StatementResult.newBuilder();
            if (Kind.DDL.contains(implementation.getKind())) {
                resultBuilder.setScalar(1);
                return resultBuilder.build();
            }
            if (Kind.DML.contains(implementation.getKind())) {
                resultBuilder.setScalar(implementation.getRowsChanged(statement));
                client.commitCurrentTransactionIfAuto();
                return resultBuilder.build();
            }

            Frame frame = StatementUtils.relationalFetch(this, 0);
            resultBuilder.setFrame(frame);
            if (frame.getIsLast()) {
                //TODO TH: special handling for result set updates. Do we need to wait with committing until all changes have been done?
                client.commitCurrentTransactionIfAuto();
            }
            return resultBuilder.build();
        }
    }

    @Override
    public PolyImplementation<PolyValue> getImplementation() {
        return null;
    }

    @Override
    public void setImplementation(PolyImplementation<PolyValue> implementation) {
        this.implementation = implementation;
    }

    @Override
    public Statement getStatement() {
        return null;
    }

    @Override
    public String getQuery() {
        return null;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public void setParameterMetas(List<ParameterMeta> parameterMetas) {
        this.parameterMetas = parameterMetas;
    }


    static class Builder {
        int id;
        PIClient client;
        QueryLanguage language;
        String query;
        PIStatementProperties properties;

        public Builder setId(int id) {
            this.id = id;
            return this;
        }


        public Builder setClient(PIClient client) {
            this.client = client;
            return this;
        }


        public Builder setLanguage(QueryLanguage language) {
            this.language = language;
            return this;
        }

        public Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder setProperties(PIStatementProperties properties) {
            this.properties = properties;
            return this;
        }

        public PIPreparedIndexedStatement build() {
            return new PIPreparedIndexedStatement(this);
        }
    }

}
