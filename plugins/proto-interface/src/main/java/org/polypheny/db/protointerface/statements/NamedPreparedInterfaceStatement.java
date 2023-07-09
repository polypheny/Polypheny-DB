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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.NamedValueProcessor;
import org.polypheny.db.protointerface.ProtoInterfaceClient;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.type.entity.PolyValue;

public class NamedPreparedInterfaceStatement extends IndexedPreparedInterfaceStatement {

    private final NamedValueProcessor namedValueProcessor;


    public NamedPreparedInterfaceStatement( int statementId, ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        super( statementId, protoInterfaceClient, queryLanguage, query );
        this.namedValueProcessor = new NamedValueProcessor( query );
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

}
