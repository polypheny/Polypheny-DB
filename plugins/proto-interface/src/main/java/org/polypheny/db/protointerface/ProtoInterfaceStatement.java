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

package org.polypheny.db.protointerface;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.proto.QueryResult;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

import java.util.Iterator;

@Slf4j
public class ProtoInterfaceStatement {

    private final StopWatch executionStopWatch;
    private final QueryLanguage queryLanguage;
    private final int statementId;
    private final ProtoInterfaceClient protoInterfaceClient;
    private String query;
    private List<Map<String, PolyValue>> valuesMaps;
    private PolyImplementation polyImplementation;
    private Statement statement;


    /* The constructor of ProtoInterfaceStatement should never be invoked directly.
     * Use StatementManager.createStatement(ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage) instead.
     */
    public ProtoInterfaceStatement( int statementId, ProtoInterfaceClient protoInterfaceClient, QueryLanguage queryLanguage, String query ) {
        this.statementId = statementId;
        this.protoInterfaceClient = protoInterfaceClient;
        this.queryLanguage = queryLanguage;
        this.query = query;
        this.executionStopWatch = new StopWatch();
    }


    public void addValues( List<Map<String, PolyValue>> valuesMaps ) {
        this.valuesMaps = valuesMaps;
    }


    public ProtoInterfaceClient getClient() {
        return protoInterfaceClient;
    }


    public QueryResult prepareAndExecute( String preparedStatement ) {
        synchronized ( protoInterfaceClient ) {
            statement = protoInterfaceClient.getCurrentOrCreateNewTransaction().createStatement();
            prepare( preparedStatement );
            return execute();
        }
    }


    public void prepare( String preparedStatement ) {
        // TODO TH: implement parameters for prepared statement
        if ( statement == null ) {
            throw new NullPointerException( "Statement must not be null." );
        }
        this.query = preparedStatement;
        Processor sqlProcessor = statement.getTransaction().getProcessor( queryLanguage );
        Node parsedStatement = sqlProcessor.parse( preparedStatement ).get( 0 );
        if ( parsedStatement.isA( Kind.DDL ) ) {
            // TODO TH: namespace type according to language
            polyImplementation = sqlProcessor.prepareDdl( statement, parsedStatement, new QueryParameters( preparedStatement, NamespaceType.RELATIONAL ) );
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( protoInterfaceClient.getCurrentTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, null );
            AlgDataType parameterRowType = sqlProcessor.getParameterRowType( validated.left );
            polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, parameterRowType, true );
        }
    }


    private QueryResult execute() {
        if ( polyImplementation == null ) {
            throw new ProtoInterfaceServiceException( "Statements must be prepared before execution." );
        }
        QueryResult.Builder resultBuilder = QueryResult.newBuilder();
        if ( Kind.DDL.contains( polyImplementation.getKind() ) ) {
            commitElseRollback();
            // TODO TH: Proper implementation
            return null;
        }
        if ( Kind.DML.contains( polyImplementation.getKind() ) ) {
            commitElseRollback();
            // TODO: proper implementation
            return null;
        }
        throw new NotImplementedException( "At this time only DML and DDL statements are implemented" );
    }


    private int getChangedRowCount() {
        Bindable<?> bindable = polyImplementation.getBindable();
        DataContext dataContext = statement.getDataContext();
        Iterator<?> iterator = PolyImplementation.enumerable( bindable, dataContext ).iterator();
        int rowsChanged = -1;
        try {
            rowsChanged = PolyImplementation.getRowsChanged( statement, iterator, statement.getMonitoringEvent().getMonitoringType() );
        } catch ( Exception e ) {
            log.error( "Caught exception while retrieving row count", e );
        }
        return rowsChanged;
    }


    private void commitElseRollback() {
        try {
            protoInterfaceClient.commitCurrentTransaction();
        } catch ( Exception e ) {
            protoInterfaceClient.rollbackCurrentTransaction();
        }
    }


}
