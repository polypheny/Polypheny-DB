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

import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.NamedValueProcessor;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.type.entity.PolyValue;

import java.util.List;
import java.util.Map;

public class PIPreparedNamedStatement extends PIPreparedIndexedStatement {

    private final NamedValueProcessor namedValueProcessor;


    public PIPreparedNamedStatement(Builder builder ) {
        super( builder );
        this.namedValueProcessor = new NamedValueProcessor( builder.query );
        overwriteQuery( namedValueProcessor.getProcessedQuery() );
    }


    public List<ParameterMeta> determineParameterMeta() {
        AlgDataType parameters = getParameterRowType();
        return RelationalMetaRetriever.retrieveParameterMetas( parameters, namedValueProcessor.getNamedIndexes() );
    }


    private void setParameters( Map<String, PolyValue> values ) {
        List<PolyValue> valueList = namedValueProcessor.transformValueMap( values );
        long index = 0;
        for ( PolyValue value : valueList ) {
            if ( value != null ) {
                currentStatement.getDataContext().addParameterValues( index++, null, List.of( value ) );
            }
        }
        hasParametersSet = true;
    }


    public StatementResult execute( Map<String, PolyValue> values ) throws Exception {
        if ( currentStatement == null ) {
            currentStatement = protoInterfaceClient.getCurrentOrCreateNewTransaction().createStatement();
        }
        setParameters( values );
        return execute();
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    static class Builder extends PIPreparedIndexedStatement.Builder {

        private Builder() {
            super();
        }

        public Builder setStatementId(int statementId) {
            this.statementId = statementId;
            return this;
        }


        public Builder setProtoInterfaceClient(PIClient protoInterfaceClient) {
            this.protoInterfaceClient = protoInterfaceClient;
            return this;
        }


        public Builder setQueryLanguage(QueryLanguage queryLanguage) {
            this.queryLanguage = queryLanguage;
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

        public PIPreparedNamedStatement build() {
            return new PIPreparedNamedStatement(this);
        }
    }

}
