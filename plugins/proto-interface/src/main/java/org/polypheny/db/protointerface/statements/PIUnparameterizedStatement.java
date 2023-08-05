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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.statementProcessing.StatementProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class PIUnparameterizedStatement extends PIStatement {

    String query;
    Statement statement;
    PolyImplementation<PolyValue> implementation;


    private PIUnparameterizedStatement( Builder builder ) {
        super(
                builder.id,
                builder.client,
                builder.properties,
                builder.language,
                builder.namespace
        );
        this.query = builder.query;
    }


    public StatementResult execute() throws Exception {
        statement = client.getCurrentOrCreateNewTransaction().createStatement();
        synchronized ( client ) {
            StatementProcessor.execute( this );
            return StatementProcessor.getResult( this );
        }
    }


    public static Builder newBuilder() {
        return new Builder();
    }


    @Override
    public PolyImplementation<PolyValue> getImplementation() {
        return implementation;
    }


    @Override
    public void setImplementation( PolyImplementation<PolyValue> implementation ) {
        this.implementation = implementation;
    }


    @Override
    public Statement getStatement() {
        return statement;
    }


    @Override
    public String getQuery() {
        return query;
    }


    static class Builder {

        int id;
        PIClient client;
        QueryLanguage language;
        String query;
        PIStatementProperties properties;
        LogicalNamespace namespace;


        public Builder setId( int id ) {
            this.id = id;
            return this;
        }


        public Builder setClient( PIClient client ) {
            this.client = client;
            return this;
        }


        public Builder setLanguage( QueryLanguage language ) {
            this.language = language;
            return this;
        }


        public Builder setQuery( String query ) {
            this.query = query;
            return this;
        }


        public Builder setProperties( PIStatementProperties properties ) {
            this.properties = properties;
            return this;
        }

        public Builder setNamespace( LogicalNamespace logicalNamespace) {
            this.namespace = logicalNamespace;
            return this;
        }


        public PIUnparameterizedStatement build() {
            return new PIUnparameterizedStatement( this );
        }

    }

}
