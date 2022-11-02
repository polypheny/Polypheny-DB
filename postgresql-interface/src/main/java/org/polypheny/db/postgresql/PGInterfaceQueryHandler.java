/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.postgresql;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;


/**
 * Handles all queries from the extended query cycle - "sends" them to polypheny and processes answer
 */
@Slf4j
public class PGInterfaceQueryHandler {

    private String query;
    private PGInterfacePreparedMessage preparedMessage;
    private Boolean preparedQueryCycle = false;
    private final ChannelHandlerContext ctx;
    private final PGInterfaceInboundCommunicationHandler communicationHandler;
    private final TransactionManager transactionManager;
    private int rowsAffected = 0;   //rows affected (changed/deleted/inserted/etc)
    private List<List<Object>> rows;
    private final PGInterfaceErrorHandler errorHandler;


    public PGInterfaceQueryHandler( String query, ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler communicationHandler, TransactionManager transactionManager ) {
        this.query = query;
        this.ctx = ctx;
        this.communicationHandler = communicationHandler;
        this.transactionManager = transactionManager;
        this.errorHandler = new PGInterfaceErrorHandler( ctx, communicationHandler );
    }

    public PGInterfaceQueryHandler( PGInterfacePreparedMessage preparedMessage, ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler communicationHandler, TransactionManager transactionManager ) {
        this.preparedMessage = preparedMessage;
        this.ctx = ctx;
        this.communicationHandler = communicationHandler;
        this.transactionManager = transactionManager;
        preparedQueryCycle = true;
        this.errorHandler = new PGInterfaceErrorHandler( ctx, communicationHandler );
    }

    /**
     * Depending on how the PGInterfaceQueryHandler was created, it sets the query and starts the process of "sending" the query to polypheny
     */
    public void start() {
        if ( preparedQueryCycle ) {
            this.query = preparedMessage.getQuery();
        }
        sendQueryToPolypheny();

    }


    /**
     * Forwards the message to Polypheny, and get result from it
     */
    public void sendQueryToPolypheny() {
        String type = "";   //query type according to answer tags
        String commitStatus;
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;
        ArrayList<String[]> data = new ArrayList<>();
        ArrayList<String[]> header = new ArrayList<>();

        try {
            //get transaction and statement
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Index Manager" );
            statement = transaction.createStatement();
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            //TODO(FF): will it continue to send things to the client?
            errorHandler.sendSimpleErrorMessage( "Error while starting transaction" + e );
            throw new RuntimeException( "Error while starting transaction", e );
        }

        try {
            if ( preparedQueryCycle ) {
                preparedMessage.transformDataAndAddParameterValues( statement );
            }

            //get algRoot
            Processor sqlProcessor = statement.getTransaction().getProcessor( Catalog.QueryLanguage.SQL );
            Node sqlNode = sqlProcessor.parse( query ).get( 0 );
            QueryParameters parameters = new QueryParameters( query, Catalog.NamespaceType.RELATIONAL );

            if ( sqlNode.isA( Kind.DDL ) ) {
                result = sqlProcessor.prepareDdl( statement, sqlNode, parameters );
                type = sqlNode.getKind().name();
                sendResultToClient( type, data, header );

            } else {
                AlgRoot algRoot = sqlProcessor.translate(
                        statement,
                        sqlProcessor.validate( statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ).left,
                        new QueryParameters( query, Catalog.NamespaceType.RELATIONAL ) );

                //get PolyResult from AlgRoot
                final QueryProcessor processor = statement.getQueryProcessor();
                result = processor.prepareQuery( algRoot, true );

                //get type information
                header = getHeader( result );

                //get actual result of query in array
                rows = result.getRows( statement, -1 );
                data = computeResultData( rows, header );

                //type = result.getStatementType().toString();
                type = result.getKind().name();

                transaction.commit();
                commitStatus = "Committed";

                sendResultToClient( type, data, header );
            }

        } catch ( Throwable t ) {
            List<PGInterfaceErrorHandler> lol = null;

            //TODO(FF): will continue to send things to client after this?
            String errorMsg = t.getMessage();
            errorHandler.sendSimpleErrorMessage( errorMsg );
            try {
                transaction.rollback();
                commitStatus = "Rolled back";
            } catch ( TransactionException ex ) {
                errorHandler.sendSimpleErrorMessage( "Error while rolling back" );
                commitStatus = "Error while rolling back";
            }
        }

    }

    /**
     * Gets the information for the header - Information for each column
     *
     * @param result the PolyImplementation the additional information is needed for
     * @return a list with array, where:
     * - array[0] = columnName
     * - array[1] = columnType
     * - array[2] = precision
     */
    private ArrayList<String[]> getHeader( PolyImplementation result ) {
        ArrayList<String[]> header = new ArrayList<>();
        for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
            String columnName = metaData.getName();
            String dataType = metaData.getType().getPolyType().getTypeName();   //INTEGER, VARCHAR
            int precision = metaData.getType().getPrecision();  //sizeVarChar, decimal places double
            boolean nullable = metaData.getType().isNullable() == (ResultSetMetaData.columnNullable == 1);

            //For each column: If it should be filtered empty string if it should not be filtered
            /*
            String filter = "";
            if ( request.filter != null && request.filter.containsKey( columnName ) ) {
                filter = request.filter.get( columnName );
            }
             */

            header.add( new String[]{ columnName, dataType, String.valueOf( precision ) } );
        }
        return header;
    }


    /**
     * Transforms the data into Strings. Possible to expand and change it into other datatypes
     *
     * @param rows   The result-data as object-type
     * @param header Header-data - additional information about the data (rows)
     * @return the rows transformed accordingly (right now turned into a string)
     */
    private ArrayList<String[]> computeResultData( List<List<Object>> rows, ArrayList<String[]> header ) {
        //TODO(FF): Implement more Datatypes
        ArrayList<String[]> data = new ArrayList<>();

        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    switch ( header.get( counter )[0] ) {
                        case "TIMESTAMP":
                            break;
                        case "DATE":
                            break;
                        case "TIME":
                            break;
                        case "FILE":
                        case "IMAGE":
                        case "SOUND":
                        case "VIDEO":
                            break;
                        //fall through
                        default:
                            temp[counter] = o.toString();
                    }
                    if ( header.get( counter )[0].endsWith( "ARRAY" ) ) {

                    }
                }
                counter++;
            }
            data.add( temp );
        }

        return data;
    }


    /**
     * Prepares according to the query from the client what (and how) should be sent as a response
     *
     * @param type   Type of the query (e.g.: Select, Insert, Create Table, etc.)
     * @param data   The data that needs to be sent to the client
     * @param header Additional information for the data
     */
    public void sendResultToClient( String type, ArrayList<String[]> data, ArrayList<String[]> header ) {
        //TODO(FF): handle more responses to client
        switch ( type ) {
            case "INSERT":
            case "DROP_TABLE":
            case "TRUNCATE":
            case "UPDATE":
                communicationHandler.sendParseBindComplete();
                communicationHandler.sendCommandComplete( type, rowsAffected );
                communicationHandler.sendReadyForQuery( "I" );

                break;

            case "CREATE_TABLE":
                //1....2....n....C....CREATE TABLE.Z....I
                communicationHandler.sendParseBindComplete();
                communicationHandler.sendCommandComplete( type, -1 );
                communicationHandler.sendReadyForQuery( "I" );

                break;

            case "SELECT":
                ArrayList<Object[]> valuesPerCol = new ArrayList<Object[]>();

                //More info about these variables in javadoc for PGInterfaceServerWriter > writeRowDescription
                String fieldName = "";          //string - column name (field name) (matters)
                int objectIDTable = 0;          //int32 - ObjectID of table (if col can be id'd to table) --> otherwise 0 (doesn't matter to client while sending)
                int attributeNoCol = 0;         //int16 - attr.no of col (if col can be id'd to table) --> otherwise 0 (doesn't matter to client while sending)
                int objectIDColDataType = 0;    //int32 - objectID of parameter datatype --> 0 = unspecified (doesn't matter to client while sending, but maybe later) - see comment below
                int dataTypeSize = 0;           //int16 - size of dataType (if formatCode = 1, this needs to be set for colValLength) (doesn't matter to client while sending)
                int typeModifier = -1;          //int32 - The value will generally be -1 (doesn't matter to client while sending)
                int formatCode = 0;             //int16 - 0: Text | 1: Binary --> sends everything with writeBytes(formatCode = 0), if sent with writeInt it needs to be 1 (matters)

                /*
                There is no list for the OID's of the data types in the postgres documentation.
                This list is a hardcoded list from the JDBC driver which contains all values.
                One element in the list is a list of these elements: {pgName, OID, sqlType, javaClass, ?}

                private static final Object[][] types = new Object[][]{{"int2", 21, 5, "java.lang.Integer", 1005}, {"int4", 23, 4, "java.lang.Integer", 1007}, {"oid", 26, -5, "java.lang.Long", 1028}, {"int8", 20, -5, "java.lang.Long", 1016}, {"money", 790, 8, "java.lang.Double", 791}, {"numeric", 1700, 2, "java.math.BigDecimal", 1231}, {"float4", 700, 7, "java.lang.Float", 1021}, {"float8", 701, 8, "java.lang.Double", 1022}, {"char", 18, 1, "java.lang.String", 1002}, {"bpchar", 1042, 1, "java.lang.String", 1014}, {"varchar", 1043, 12, "java.lang.String", 1015}, {"text", 25, 12, "java.lang.String", 1009}, {"name", 19, 12, "java.lang.String", 1003}, {"bytea", 17, -2, "[B", 1001}, {"bool", 16, -7, "java.lang.Boolean", 1000}, {"bit", 1560, -7, "java.lang.Boolean", 1561}, {"date", 1082, 91, "java.sql.Date", 1182}, {"time", 1083, 92, "java.sql.Time", 1183}, {"timetz", 1266, 92, "java.sql.Time", 1270}, {"timestamp", 1114, 93, "java.sql.Timestamp", 1115}, {"timestamptz", 1184, 93, "java.sql.Timestamp", 1185}, {"refcursor", 1790, 2012, "java.sql.ResultSet", 2201}, {"json", 114, 1111, "org.postgresql.util.PGobject", 199}, {"point", 600, 1111, "org.postgresql.geometric.PGpoint", 1017}};

                 */

                //data
                int numberOfFields = header.size(); //int16 - number of fields (cols) (matters)

                for ( String[] head : header ) {

                    fieldName = head[0];

                    //TODO(FF): Implement the rest of the cases - only Integer and varchar tested --> warn client?
                    switch ( head[1] ) {
                        case "BIGINT":
                        case "DOUBLE":
                            //TODO(FF): head[2] is the number of decimal places, is set to 3 in standard postgres ("dismissed in beginning, not checked what it actually is")
                            dataTypeSize = 8;   //8 bytes signed
                            formatCode = 0;
                            objectIDColDataType = 20;
                            break;
                        case "BOOLEAN":
                            dataTypeSize = 1;   //TODO(FF): how exactly is bool sent? acc. to doc. size is 1 bit?
                            objectIDColDataType = 16;
                            break;
                        case "DECIMAL":
                            break;
                        case "REAL":
                        case "INTEGER":
                            objectIDColDataType = 23;
                            dataTypeSize = 4;
                            formatCode = 0;
                            break;
                        case "VARCHAR":
                            objectIDColDataType = 1043;
                            typeModifier = Integer.parseInt( head[2] );
                            dataTypeSize = Integer.parseInt( head[2] ); //TODO(FF): I just send the length of the varchar here, because the client doesn't complain.
                            formatCode = 0;
                            break;
                        case "SMALLINT":
                            objectIDColDataType = 21;
                            dataTypeSize = 2;
                            formatCode = 0;
                            break;
                        case "TINYINT":
                            objectIDColDataType = 21;   //is the same oID as for int2
                            dataTypeSize = 1;
                            formatCode = 0;
                            break;
                        case "DATE":            //I did not find a list online for all OID's --> more info in comment on init. of oid's
                        case "TIMESTAMP":
                        case "TIME":
                        case "FILE":
                        case "IMAGE":
                        case "SOUND":
                        case "VIDEO":
                        default:
                            errorHandler.sendSimpleErrorMessage( "The DataType of the answer is not yet implemented, but there is a high chance that the query was executed in Polypheny" );
                            break;
                    }
                    Object[] col = { fieldName, objectIDTable, attributeNoCol, objectIDColDataType, dataTypeSize, typeModifier, formatCode };
                    valuesPerCol.add( col );
                }
                communicationHandler.sendParseBindComplete();
                communicationHandler.sendRowDescription( numberOfFields, valuesPerCol );
                communicationHandler.sendDataRow( data );

                rowsAffected = data.size();
                communicationHandler.sendCommandComplete( type, rowsAffected );
                communicationHandler.sendReadyForQuery( "I" );

                break;

            case "DELETE":
                //DELETE rows (rows = #rows deleted)

            case "MOVE":
                //MOVE rows (rows = #rows the cursor's position has been changed by (??))

            case "FETCH":
                //FETCH rows (rows = #rows that have been retrieved from cursor)

            case "COPY":
                //COPY rows (rows = #rows copied --> only on PSQL 8.2 and later)$

            default:
                errorHandler.sendSimpleErrorMessage( "Answer to client is not yet supported, but there is a high chance that the query was executed in Polypheny" );
                break;

        }
    }
}
