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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.ProtoInterfaceClient;
import org.polypheny.db.protointerface.ProtoInterfaceServiceException;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class IndexedPreparedInterfaceStatement extends ProtoInterfaceStatement implements Signaturizable {

    protected Statement currentStatement;
    protected boolean hasParametersSet;


    public IndexedPreparedInterfaceStatement( int statementId, ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        super( statementId, protoInterfaceClient, queryLanguage, query );
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
            throw new ProtoInterfaceServiceException( "Can't execute parameterized statement before preparation." );
        }
        return execute( currentStatement );
    }

}
