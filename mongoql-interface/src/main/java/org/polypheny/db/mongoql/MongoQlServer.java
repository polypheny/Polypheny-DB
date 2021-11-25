/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.mongoql;


import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Family;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.mongoql.model.DbColumn;
import org.polypheny.db.mongoql.model.QueryRequest;
import org.polypheny.db.mongoql.model.Result;
import org.polypheny.db.mongoql.model.SortState;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DateTimeStringUtils;
import org.polypheny.db.util.Util;
import spark.Request;
import spark.Response;
import spark.Service;


@Slf4j
public class MongoQlServer extends QueryInterface {

    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_NAME = "MongoQL Interface";
    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_DESCRIPTION = "MongoQL-based query interface.";
    @SuppressWarnings("WeakerAccess")
    public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new QueryInterfaceSettingInteger( "port", false, true, false, 2717 ),
            new QueryInterfaceSettingInteger( "maxUploadSizeMb", false, true, true, 10000 )
    );

    private final Gson gson = new Gson();

    private final int port;
    private final String uniqueName;

    // Counter
    private final AtomicLong deleteCounter = new AtomicLong();
    private final AtomicLong getCounter = new AtomicLong();
    private final AtomicLong patchCounter = new AtomicLong();
    private final AtomicLong postCounter = new AtomicLong();

    private final MonitoringPage monitoringPage;

    private Service restServer;


    public MongoQlServer( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, false );
        this.uniqueName = uniqueName;
        this.port = Integer.parseInt( settings.get( "port" ) );
        if ( !Util.checkIfPortIsAvailable( port ) ) {
            // Port is already in use
            throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
        }
        // Add information page
        monitoringPage = new MonitoringPage();
    }


    @Override
    public void run() {
        restServer = Service.ignite();
        restServer.port( port );

        restServer.post( "/mongo", this::anyQuery );
        log.info( "{} started and is listening on port {}.", INTERFACE_NAME, port );
    }


    /**
     * This method handles mql queries of any sort
     *
     * // DISCLAIMER: this interface and this method only handle MQL atm
     * // but will be repurposed later on in a more general http language server
     *
     * @param request the incoming request
     * @param res the outgoing response
     * @return the result of the executed query
     */
    public String anyQuery( final Request request, final Response res ) {
        QueryRequest query = gson.fromJson( request.body(), QueryRequest.class );

        Transaction transaction = getTransaction();

        PolyphenyDbSignature<?> signature;
        MqlProcessor mqlProcessor = (MqlProcessor) transaction.getProcessor( QueryLanguage.MONGOQL );
        String mql = query.query;
        Statement statement = transaction.createStatement();

        // This is not a nice solution. In case of a mql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten

        List<Result> results = new ArrayList<>();

        MqlNode parsed = (MqlNode) mqlProcessor.parse( mql );

        QueryParameters parameters = new MqlQueryParameters( mql, query.database );

        if ( parsed.getFamily() == Family.DML && mqlProcessor.needsDdlGeneration( parsed, parameters ) ) {
            mqlProcessor.autoGenerateDDL( getTransaction().createStatement(), parsed, parameters );
        }

        long executionTime = System.nanoTime();


        try {
            if ( parsed.getFamily() == Family.DDL ) {
                mqlProcessor.prepareDdl( statement, parsed, parameters );
                Result result = new Result( 1 ).setGeneratedQuery( mql ).setXid( statement.getTransaction().getXid().toString() );
                results.add( result );
            } else {
                RelRoot logicalRoot = mqlProcessor.translate( statement, parsed, parameters );

                // Prepare
                signature = statement.getQueryProcessor().prepareQuery( logicalRoot );

                results = getResults( statement, query, signature );
            }
            executionTime = System.nanoTime() - executionTime;
            try {
                statement.getTransaction().commit();
            } catch ( TransactionException e ) {
                throw new RuntimeException( "error while committing" );
            }

            return gson.toJson( results.get( 0 ) );
        } catch ( Exception e ) {
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                throw new RuntimeException( "error happened during rollback" );
            }
            return gson.toJson( e.toString() );
        }
    }


    private List<Result> getResults( Statement statement, QueryRequest request, PolyphenyDbSignature<?> signature ) {
        Catalog catalog = Catalog.getInstance();

        Iterator<Object> iterator;
        boolean hasMoreRows;
        List<List<Object>> rows;
        final Enumerable enumerable = signature.enumerable( statement.getDataContext() );
        //noinspection unchecked
        iterator = enumerable.iterator();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );

        hasMoreRows = iterator.hasNext();
        stopWatch.stop();
        signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );

        try {
            CatalogTable catalogTable = null;
            if ( request.tableId != null ) {
                String[] t = request.tableId.split( "\\." );
                try {
                    catalogTable = Catalog.getInstance().getTable( statement.getPrepareContext().getDefaultSchemaName(), t[0], t[1] );
                } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                    log.error( "Caught exception", e );
                }
            }

            ArrayList<DbColumn> header = new ArrayList<>();
            for ( ColumnMetaData metaData : signature.columns ) {
                String columnName = metaData.columnName;

                String filter = "";
                if ( request.filter != null && request.filter.containsKey( columnName ) ) {
                    filter = request.filter.get( columnName );
                }

                SortState sort;
                if ( request.sortState != null && request.sortState.containsKey( columnName ) ) {
                    sort = request.sortState.get( columnName );
                } else {
                    sort = new SortState();
                }

                DbColumn dbCol = new DbColumn(
                        metaData.columnName,
                        metaData.type.name,
                        metaData.nullable == ResultSetMetaData.columnNullable,
                        metaData.displaySize,
                        sort,
                        filter );

                // Get column default values
                if ( catalogTable != null ) {
                    try {
                        if ( catalog.checkIfExistsColumn( catalogTable.id, columnName ) ) {
                            CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                            if ( catalogColumn.defaultValue != null ) {
                                dbCol.defaultValue = catalogColumn.defaultValue.value;
                            }
                        }
                    } catch ( UnknownColumnException e ) {
                        log.error( "Caught exception", e );
                    }
                }
                header.add( dbCol );
            }

            ArrayList<String[]> data = computeResultData( rows, header, statement.getTransaction() );

            return Collections.singletonList( new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ) ).setAffectedRows( data.size() ).setHasMoreRows( hasMoreRows ) );
        } finally {
            try {
                ((AutoCloseable) iterator).close();
            } catch ( Exception e ) {
                log.error( "Exception while closing result iterator", e );
            }
        }
    }


    /**
     * Convert data from a query result to Strings readable in the UI
     *
     * @param rows Rows from the enumerable iterator
     * @param header Header from the UI-ResultSet
     */
    public ArrayList<String[]> computeResultData( final List<List<Object>> rows, final List<DbColumn> header, final Transaction transaction ) {
        ArrayList<String[]> data = new ArrayList<>();
        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    switch ( header.get( counter ).dataType ) {
                        case "TIMESTAMP":
                            if ( o instanceof Long ) {
                                temp[counter] = DateTimeStringUtils.longToAdjustedString( (long) o, PolyType.TIMESTAMP );// TimestampString.fromMillisSinceEpoch( (long) o ).toString();
                            } else {
                                temp[counter] = o.toString();
                            }
                            break;
                        case "DATE":
                            if ( o instanceof Integer ) {
                                temp[counter] = DateTimeStringUtils.longToAdjustedString( (int) o, PolyType.DATE );//DateString.fromDaysSinceEpoch( (int) o ).toString();
                            } else {
                                temp[counter] = o.toString();
                            }
                            break;
                        case "TIME":
                            if ( o instanceof Integer ) {
                                temp[counter] = DateTimeStringUtils.longToAdjustedString( (int) o, PolyType.TIME );//TimeString.fromMillisOfDay( (int) o ).toString();
                            } else {
                                temp[counter] = o.toString();
                            }
                            break;
                        case "FILE":
                        case "IMAGE":
                        case "SOUND":
                        case "VIDEO":
                            //fall through
                        default:
                            temp[counter] = o.toString();
                    }
                    if ( header.get( counter ).dataType.endsWith( "ARRAY" ) ) {
                        if ( o instanceof Array ) {
                            try {
                                temp[counter] = gson.toJson( ((Array) o).getArray(), Object[].class );
                            } catch ( SQLException sqlException ) {
                                temp[counter] = o.toString();
                            }
                        } else if ( o instanceof List ) {
                            // TODO js(knn): make sure all of this is not just a hotfix.
                            temp[counter] = gson.toJson( o );
                        } else {
                            temp[counter] = o.toString();
                        }
                    }
                }
                counter++;
            }
            data.add( temp );
        }
        return data;
    }


    private Transaction getTransaction() {
        Transaction transaction = null;
        try {
            transaction = transactionManager.startTransaction( Catalog.getInstance().getUser( Catalog.defaultUser ).name, "APP", false, "Polypheny-MongoQL", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            e.printStackTrace();
        }
        return transaction;
    }


    @Override
    public List<QueryInterfaceSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public void shutdown() {
        restServer.stop();
    }


    @Override
    public String getInterfaceType() {
        return "MongoQL";
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }


    private class MonitoringPage {

        private final InformationPage informationPage;
        private final InformationGroup informationGroupRequests;
        private final InformationGraph counterGraph;
        private final InformationTable counterTable;


        public MonitoringPage() {
            InformationManager im = InformationManager.getInstance();

            informationPage = new InformationPage( uniqueName, INTERFACE_NAME ).fullWidth().setLabel( "Interfaces" );
            informationGroupRequests = new InformationGroup( informationPage, "Requests" );

            im.addPage( informationPage );
            im.addGroup( informationGroupRequests );

            counterGraph = new InformationGraph(
                    informationGroupRequests,
                    GraphType.DOUGHNUT,
                    new String[]{ "DELETE", "GET", "PATCH", "POST" }
            );
            counterGraph.setOrder( 1 );
            im.registerInformation( counterGraph );

            counterTable = new InformationTable(
                    informationGroupRequests,
                    Arrays.asList( "Type", "Percent", "Absolute" )
            );
            counterTable.setOrder( 2 );
            im.registerInformation( counterTable );

            informationGroupRequests.setRefreshFunction( this::update );
        }


        public void update() {
            long deleteCount = deleteCounter.get();
            long getCount = getCounter.get();
            long patchCount = patchCounter.get();
            long postCount = postCounter.get();
            double total = deleteCount + getCount + patchCount + postCount;

            counterGraph.updateGraph(
                    new String[]{ "DELETE", "GET", "PATCH", "POST" },
                    new GraphData<>( "requests", new Long[]{ deleteCount, getCount, patchCount, postCount } )
            );

            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
            symbols.setDecimalSeparator( '.' );
            DecimalFormat df = new DecimalFormat( "0.0", symbols );
            counterTable.reset();
            counterTable.addRow( "DELETE", df.format( total == 0 ? 0 : (deleteCount / total) * 100 ) + " %", deleteCount );
            counterTable.addRow( "GET", df.format( total == 0 ? 0 : (getCount / total) * 100 ) + " %", getCount );
            counterTable.addRow( "PATCH", df.format( total == 0 ? 0 : (patchCount / total) * 100 ) + " %", patchCount );
            counterTable.addRow( "POST", df.format( total == 0 ? 0 : (postCount / total) * 100 ) + " %", postCount );
        }


        public void remove() {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( counterGraph, counterTable );
            im.removeGroup( informationGroupRequests );
            im.removePage( informationPage );
        }

    }

}
