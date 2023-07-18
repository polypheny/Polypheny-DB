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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class PIPreparedIndexedStatement extends PIStatement implements Signaturizable, PIStatementBatch {

    protected Statement currentStatement;
    protected boolean hasParametersSet;


    protected PIPreparedIndexedStatement(Builder builder) {
        super( builder );
        this.hasParametersSet = false;
    }

    protected AlgDataType getParameterRowType() {
        Transaction transaction = protoInterfaceClient.getCurrentOrCreateNewTransaction();
        currentStatement = transaction.createStatement();
        Processor queryProcessor = transaction.getProcessor( queryLanguage );
        Node parsed = queryProcessor.parse( query ).get( 0 );
        // It is important not to add default values for missing fields in insert statements. If we did this, the
        // JDBC driver would expect more parameter fields than there actually are in the query.
        Pair<Node, AlgDataType> validated = queryProcessor.validate( transaction, parsed, false );
        return queryProcessor.getParameterRowType( validated.left );
    }


    public List<ParameterMeta> determineParameterMeta() {
        AlgDataType parameterRowType = getParameterRowType();
        return RelationalMetaRetriever.retrieveParameterMetas( parameterRowType );
    }


    private void setParameters( List<PolyValue> values ) {
        long index = 0;
        for ( PolyValue value : values ) {
            if ( value != null ) {
                currentStatement.getDataContext().addParameterValues( index++, null, List.of( value ) );
            }
        }
        this.hasParametersSet = true;
    }

    public StatementResult execute( List<PolyValue> values ) throws Exception {
        if ( currentStatement == null ) {
            currentStatement = protoInterfaceClient.getCurrentOrCreateNewTransaction().createStatement();
        }
        setParameters( values );
        return execute();
    }


    @Override
    public StatementResult execute() throws Exception {
        if ( currentStatement == null || !hasParametersSet ) {
            throw new PIServiceException( "Can't execute parameterized statement before preparation." );
        }
        return execute( currentStatement );
    }


    public List<Long> executeBatch(List<List<PolyValue>> valueLists) throws Exception {
        List<Long> updateCounts = new LinkedList<>();
        for (List<PolyValue> values : valueLists) {
            updateCounts.add( execute(values).getScalar() );
        }
        return updateCounts;
    }


    @Override
    public List<PIStatement> getStatements() {
        return Collections.singletonList( this );
    }


    @Override
    public int getBatchId() {
        // As prepared statements implement tProtoInterfaceStatementBatch directly, thy don't have a separate batch id.
        return statementId;
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    static class Builder extends PIStatement.Builder {

        protected Builder() {
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

        public PIPreparedIndexedStatement build() {
            return new PIPreparedIndexedStatement(this);
        }
    }

}
