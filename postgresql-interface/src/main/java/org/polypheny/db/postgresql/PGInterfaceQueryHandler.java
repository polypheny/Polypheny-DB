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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
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
import org.polypheny.db.type.PolyType;


@Slf4j
public class PGInterfaceQueryHandler {

    private String query;
    private PGInterfacePreparedMessage preparedMessage;
    private Boolean preparedQueryCycle = false;
    private ChannelHandlerContext ctx;
    private PGInterfaceInboundCommunicationHandler communicationHandler;
    private TransactionManager transactionManager;
    private int rowsAffected = 0;   //rows affected (changed/deleted/inserted/etc)
    private List<List<Object>> rows;
    private PGInterfaceErrorHandler errorHandler;


    public PGInterfaceQueryHandler( String query, ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler communicationHandler, TransactionManager transactionManager ) {
        this.query = query;
        this.ctx = ctx;
        this.communicationHandler = communicationHandler;
        this.transactionManager = transactionManager;
        this.errorHandler = new PGInterfaceErrorHandler(ctx, communicationHandler);
    }

    public PGInterfaceQueryHandler( PGInterfacePreparedMessage preparedMessage, ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler communicationHandler, TransactionManager transactionManager ) {
        this.preparedMessage = preparedMessage;
        this.ctx = ctx;
        this.communicationHandler = communicationHandler;
        this.transactionManager = transactionManager;
        preparedQueryCycle = true;
        this.errorHandler = new PGInterfaceErrorHandler(ctx, communicationHandler);
    }

    public void start() {
        //hardcodeResponse();
        if (preparedQueryCycle) {
            this.query = preparedMessage.getQuery();
        }
        //int hash = ctx.hashCode();

        sendQueryToPolypheny();

    }

    private void hardcodeResponse() {

        ByteBuf buffer = ctx.alloc().buffer();
        ByteBuf buffer2 = ctx.alloc().buffer();
        ByteBuf buffer3 = ctx.alloc().buffer();
        ByteBuf buffer4 = ctx.alloc().buffer();
        ByteBuf buffer5 = ctx.alloc().buffer();
        /*
        1....2....T......empid...@...............D..........100C....SELECT 1.Z....I

         1 ...  .  2...  .  T ...  . .  . empid...  @  . .  . ...  . .  .  .  .  .  . ..  D ...  . .  . ...  .  1  0  0  C ...  . SELECT 1.  Z ... .  I
        31 ... 04 32... 04 54 ... 1e . 01 empid... 40 0c . 01 ... 17 . 04 ff ff ff ff .. 44 ... 0d . 01 ... 03 31 30 30 43 ... 0d SELECT 1. 5a ...05 49
                           T, 0, 1,40, 1,17,0,4             D, 0d, 0, 1, 3, 100
         */

        /*
        //parseComplete
        buffer2.writeByte('1');
        //buffer = writeIntArray(nbrs, buffer);
        buffer2.writeInt(4);
        //bindComplete
        buffer2.writeByte('2');
        //buffer = writeIntArray(nbrs2, buffer);
        buffer2.writeInt(4);
        ctx.writeAndFlush(buffer2);
         */
        communicationHandler.sendParseBindComplete();

        //RowDescription
        buffer.writeBytes("T".getBytes(StandardCharsets.UTF_8));
        //buffer.writeShort(23);
        //buffer.writeShort(24+"empid".length() +1);  //1e --> egal?
        buffer.writeInt(24+"empid".length() +1); //egal? -20, +550...
        buffer.writeShort(1);   //mues stemme --> nbr of fields?
        //buffer = writeIntArray(nbrs3, buffer);
        buffer.writeBytes("empid".getBytes(StandardCharsets.UTF_8));
        buffer.writeByte(0);    //mues 0 sii... --> wennmers onde macht, ond int zgross esch, gets en fähler...
        buffer.writeInt(1);    //@ --> egal: 654, 0, 1111111111
        //buffer.writeByte(64);
        buffer.writeShort(25);  //1 abst. zvel zwösche 40 ond 0c
        //buffer.writeByte(0);   //0c --> egal (mer cha au d reihefolg zwösche short ond byte wächsle
        //buffer.writeShort(1);   //egal
        //buffer.writeShort(0);
        //buffer.writeShort(23);  //17
        buffer.writeInt(23);  //17 --> egal: 254, 0
        buffer.writeShort(4); //egal: 400, 0
        //ctx.writeAndFlush(buffer);
        //buffer = writeIntArray(nbrs4, buffer);
        //buffer.writeShort(2147483647);  //ff ff
        //buffer.writeShort(2147483647);
        buffer.writeInt(-1);    //statt 2 short (ff wahrsch. -1?), egal: -1, 20, 2550, 0
        //buffer.writeByte(0);
        //buffer.writeByte(0);  //short statt 2 bytes
        buffer.writeShort(0);   //0 (54, 111): chonnt 1111111111 ah | 1: 825307441

        //DataRow
        buffer5.writeBytes("D".getBytes(StandardCharsets.UTF_8));
        buffer5.writeInt(20); //egal? --> 20, 200, 400, 0
        //buffer5.writeShort(0); //egal? --> 20, 200 (short met int ersetzt)
        //buffer5.writeShort(13);  //0d --> chonnt ned wörklech drufah was dren esch... (donkt mi) --> fonktioniert met 1 ond 200
        buffer5.writeShort(1);  //das mues stemme, söscht warted de client
        //buffer5.writeShort(0);    //usegnoh, ond deför onders of int gwächslet
        buffer5.writeInt("1111111111".length());  //length of the datatype --> mues stemme, sösch fähler
        //buffer5.writeInt(4);  //length of the datatype --> mues stemme, sösch fähler --> för writeInt = 4
        //buffer = writeIntArray(nbrs5, buffer);
        buffer5.writeBytes("1111111111".getBytes(StandardCharsets.UTF_8));
        //buffer5.writeInt(1111111111);

        //CommandComplete
        buffer4.writeBytes("C".getBytes(StandardCharsets.UTF_8));
        buffer4.writeShort(0);
        buffer4.writeShort(13);
        //buffer2 = writeIntArray(nbrs6, buffer2);
        buffer4.writeBytes("SELECT 1".getBytes(StandardCharsets.UTF_8));
        //buffer2 = writeIntArray(nbrs7, buffer2);
        buffer4.writeByte(0);

        //ReadyForQuery
        buffer3.writeBytes("Z".getBytes(StandardCharsets.UTF_8));
        //buffer2 = writeIntArray(nbrs8, buffer2);
        buffer3.writeShort(0);
        buffer3.writeShort(5);
        buffer3.writeBytes("I".getBytes(StandardCharsets.UTF_8));


        ctx.writeAndFlush( buffer );
        ctx.writeAndFlush( buffer5 );
        //ctx.writeAndFlush( buffer4 );
        communicationHandler.sendCommandComplete( "SELECT", 1 );
        //ctx.writeAndFlush( buffer3 );
        communicationHandler.sendReadyForQuery( "I" );

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
            //get transaction letze linie
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Index Manager" );
            statement = transaction.createStatement();
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            //TODO(FF): stop sending stuff to client...
            errorHandler.sendSimpleErrorMessage("Error while starting transaction" + String.valueOf(e));
            throw new RuntimeException( "Error while starting transaction", e );
        }

        try {
            if (preparedQueryCycle) {
                //TODO(prepared Queries): met dem denn values dezue tue
                AlgDataType algDataType = statement.getTransaction().getTypeFactory().createPolyType(PolyType.INTEGER);
                statement.getTransaction().getTypeFactory().createPolyType(PolyType.VARCHAR, 255);
                statement.getTransaction().getTypeFactory().createPolyType(PolyType.BOOLEAN);
                statement.getTransaction().getTypeFactory().createPolyType(PolyType.DECIMAL, 3, 3);
                Map<Long, AlgDataType> types = null;
                List<Map<Long, Object>> values = null;  //long index
                statement.getDataContext().setParameterTypes(types); //döfs erscht bem execute step mache...
                statement.getDataContext().setParameterValues(values);

            }
            //get algRoot  --> use it in abstract queryProcessor (prepare query) - example from catalogImpl (461-446)
            Processor sqlProcessor = statement.getTransaction().getProcessor(Catalog.QueryLanguage.SQL);
            Node sqlNode = sqlProcessor.parse(query).get(0);
            QueryParameters parameters = new QueryParameters(query, Catalog.NamespaceType.RELATIONAL);
            if (sqlNode.isA(Kind.DDL)) {
                result = sqlProcessor.prepareDdl(statement, sqlNode, parameters);
                type = sqlNode.getKind().name();
                sendResultToClient( type, data, header );

            } else {
                AlgRoot algRoot = sqlProcessor.translate(
                        statement,
                        sqlProcessor.validate(statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean()).left,
                        new QueryParameters(query, Catalog.NamespaceType.RELATIONAL));

                //get PolyResult from AlgRoot - use prepareQuery from abstractQueryProcessor (example from findUsages)
                final QueryProcessor processor = statement.getQueryProcessor();
                result = processor.prepareQuery(algRoot, true);

                //get type information - from crud.java
                header = getHeader( result );

                //get actual result of query in array - from crud.java
                rows = result.getRows( statement, -1 );
                data = computeResultData( rows, header );

                //type = result.getStatementType().toString();
                type = result.getKind().name();

                transaction.commit();


                //java.lang.RuntimeException: The table 'emps' is provided by a data source which does not support data modification.


                //committe of transaction (commitAndFinish (languageCrud)
                //transaction.commit(); (try catch --> be catch rollback


                //handle result, depending on query type
                sendResultToClient( type, data, header );

            }
        } catch (Throwable t) { //TransactionExeption?
            List <PGInterfaceErrorHandler> lol = null;
            //TODO(FF): stop sending stuff to client...
            //log.error( "Caught exception while executing query", e );
            String errorMsg = t.getMessage();
            errorHandler.sendSimpleErrorMessage(errorMsg);
            try {
                transaction.rollback();
                commitStatus = "Rolled back";
            } catch (TransactionException ex) {
                //log.error( "Could not rollback CREATE TABLE statement: {}", ex.getMessage(), ex );
                errorHandler.sendSimpleErrorMessage("Error while rolling back");
                commitStatus = "Error while rolling back";
            }
        }

    }

    /**
     * Gets the information for the header - Information for each column
     * @param result the PolyImplementation the additional information is needed for
     * @return a list with array, where:
     *         - array[0] = columnName
     *         - array[1] = columnType
     *         - array[2] = precision
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

            //For each column: If and how it should be sorted
            /*
            SortState sort;
            if ( request.sortState != null && request.sortState.containsKey( columnName ) ) {
                sort = request.sortState.get( columnName );
            } else {
                sort = new SortState();
            }
             */

            /*
            DbColumn dbCol = new DbColumn(
                    metaData.getName(),
                    metaData.getType().getPolyType().getTypeName(),
                    metaData.getType().isNullable() == (ResultSetMetaData.columnNullable == 1),
                    metaData.getType().getPrecision(),
                    sort,
                    filter );
             */

            //bruuch ich ned wörklech?
            /*
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

             */
            header.add( new String[]{ columnName, dataType, String.valueOf( precision ) } );
        }
        return header;
    }


    /**
     * Transforms the data into Strings. Possble to expand and change it into other datatypes
     * @param rows The result-data as object-type
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
     * @param type Type of the query (e.g.: Select, Insert, Create Table, etc.)
     * @param data The data that needs to be sent to the client
     * @param header Additional information for the data
     */
    public void sendResultToClient( String type, ArrayList<String[]> data, ArrayList<String[]> header ) {
        switch ( type ) {
            case "INSERT":
                //INSERT oid rows (oid=0, rows = #rows inserted)
                //1....2....n....C....INSERT 0 1.Z....I

                //insert into table with several vals (but only 1 row)
                /*
                client:
                P...J.INSERT INTO Album(AlbumId, Title, ArtistId) VALUES (1, 'Hello', 1)...B............D....P.E...	.....S....

                server:
                1....2....n....C....INSERT 0 1.Z....I
                 */

                communicationHandler.sendParseBindComplete();
                communicationHandler.sendCommandComplete( type, rowsAffected );
                communicationHandler.sendReadyForQuery( "I" );

                break;

            case "CREATE_TABLE":
                //1....2....n....C....CREATE TABLE.Z....I
                communicationHandler.sendParseBindComplete();
                //communicationHandler.sendCommandCompleteCreateTable();
                communicationHandler.sendCommandComplete( type, -1 );
                communicationHandler.sendReadyForQuery( "I" );

                break;

            case "SELECT":
                ArrayList<Object[]> valuesPerCol = new ArrayList<Object[]>();

                String fieldName = "";          //string - column name (field name) (matters)
                int objectIDTable = 0;          //int32 - ObjectID of table (if col can be id'd to table) --> otherwise 0 (doesn't matter to client while sending)
                int attributeNoCol = 0;         //int16 - attr.no of col (if col can be id'd to table) --> otherwise 0 (doesn't matter to client while sending)
                int objectIDColDataType = 0;    //int32 - objectID of parameter datatype --> 0 = unspecified (doesn't matter to client while sending)
                int dataTypeSize = 0;           //int16 - size of dataType (if formatCode = 1, this needs to be set for colValLength) (doesn't matter to client while sending)
                int typeModifier = -1;          //int32 - The value will generally be -1 (doesn't matter to client while sending)
                int formatCode = 0;             //int16 - 0: Text | 1: Binary --> sends everything with writeBytes(formatCode = 0), if sent with writeInt it needs to be 1 (matters)


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
                            break;
                        case "BOOLEAN":
                            dataTypeSize = 1;   //TODO(FF): wär 1bit --> wie das darstelle??????
                            break;
                        case "DATE":
                            break;
                        case "DECIMAL":
                            break;
                        case "REAL":
                        case "INTEGER":
                            //objectIDColDataType = 32;
                            dataTypeSize = 4;
                            formatCode = 0;
                            break;
                        case "VARCHAR":
                            //objectIDColDataType = 1043;
                            typeModifier = Integer.parseInt( head[2] );
                            dataTypeSize = Integer.parseInt( head[2] ); //TODO(FF): I just send the length of the varchar here, because the client doesn't complain.
                            formatCode = 0;
                            break;
                        case "SMALLINT":
                            dataTypeSize = 2;
                            formatCode = 0;
                            break;
                        case "TINYINT":
                            dataTypeSize = 1;
                            formatCode = 0;
                            break;
                        case "TIMESTAMP":
                            break;
                        case "TIME":
                            break;
                        case "FILE":
                        case "IMAGE":
                        case "SOUND":
                        case "VIDEO":
                            break;
                    }
                    Object col[] = { fieldName, objectIDTable, attributeNoCol, objectIDColDataType, dataTypeSize, typeModifier, formatCode };
                    valuesPerCol.add( col );
                }
                communicationHandler.sendParseBindComplete();
                communicationHandler.sendRowDescription( numberOfFields, valuesPerCol );
                communicationHandler.sendDataRow(data);

                rowsAffected = data.size();
                communicationHandler.sendCommandComplete( type, rowsAffected );
                communicationHandler.sendReadyForQuery( "I" );

                break;

            case "DELETE":
                //DELETE rows (rows = #rows deleted)
                break;

            case "MOVE":
                //MOVE rows (rows = #rows the cursor's position has been changed by (??))
                break;

            case "FETCH":
                //FETCH rows (rows = #rows that have been retrieved from cursor)
                break;

            case "COPY":
                //COPY rows (rows = #rows copied --> only on PSQL 8.2 and later)
                break;

        }
    }

    //(SELECT empid FROM public.emps LIMIT 1) in postgres
    /*
1....2....T......empid...@...............D..........100C....SELECT 1.Z....I

1...  .  2...  .  T ...  . .  . empid...  @  . .  . ...  . .  .  .  .  .  . ..  D ...  . .  . ...  .  1  0  0  C ...  . SELECT 1.  Z ... .  I
1... 04 32... 04 54 ... 1e . 01 empid... 40 0c . 01 ... 17 . 04 ff ff ff ff .. 44 ... 0d . 01 ... 03 31 30 30 43 ... 0d SELECT 1. 5a ...05 49

empid = 65 6d 70 69 64
SELECT 1 = 53 45 4c 45 43 54 20 31
(select_abst._1)
     */



    //Example of server answer to simple select query (from real server)
    /*
    1....2....T......lolid...@...............D..........1D..........2D..........3D..........3D..........3D..........3C...
SELECT 6.Z....I

(result: 1,2,3,3,3,3)
1: ParseComplete indicator
2: BindComplete indicator
T: RowDescription - specifies the number of fields in a row (can be 0) (as message content!!) - then for each field:
	field name (string),  lolid
	ObjectID of table (if field can be id'd as col of specific table, otherwise 0) (Int32), 40 --> kompliziert
	attributeNbr of col (if field can be id'd as col of specific table, otherwise 0) (Int16), 2
	ObjectID of fields data type (Int32), 1

	Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
	--> apparently specified in parse message (at the end, if 0, then unspecified...)

	data type size (negative vals = variable-width types) (see pg_type.typlen) (Int16), 17 --> polypheny website, typedokumentation, mit länge
	real and double in polypheny s gliiche --> luege was postgres macht, mind. länge aaluege --> postgresqlStore schauen welche grössen wie gemappt
	gibt methode um sql type zu holen dort --> luege wies dbms meta macht (avatica generall interface hauptklasse)

	type modifier (meaning of modifier is type-specific) (see pg_attribute.atttypmod) (Int32), 4

	Format code used for the field (zero(text) or one(binary)) --> if rowDescription is returned from statement variant of DESCRIBE: format code not yet known (always zero) (Int16)

D: DataRow - length - nbr of col values that follow (possible 0) - then for each column the pair of fields:
	length of the column value (not includes itself) (zero possible, -1: special case - NULL col val (no value bytes follow in the NULL case),
	value of the col (in format indicated by associated format code)
T: RowDescription - specifies the number of fields in a row (can be 0) (as message content!!) - then for each field:
	field name (string),  lolid
	ObjectID of table (if field can be id'd as col of specific table, otherwise 0) (Int32), 40 --> kompliziert
	attributeNbr of col (if field can be id'd as col of specific table, otherwise 0) (Int16), 2
	ObjectID of fields data type (Int32), 1

	Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
	--> apparently specified in parse message (at the end, if 0, then unspecified...)

	data type size (negative vals = variable-width types) (see pg_type.typlen) (Int16), 17 --> polypheny website, typedokumentation, mit länge
	real and double in polypheny s gliiche --> luege was postgres macht, mind. länge aaluege --> postgresqlStore schauen welche grössen wie gemappt
	gibt methode um sql type zu holen dort --> luege wies dbms meta macht (avatica generall interface hauptklasse)

	type modifier (meaning of modifier is type-specific) (see pg_attribute.atttypmod) (Int32), 4

	Format code used for the field (zero(text) or one(binary)) --> if rowDescription is returned from statement variant of DESCRIBE: format code not yet known (always zero) (Int16)

D: DataRow - length - nbr of col values that follow (possible 0) - then for each column the pair of fields: (int16)
	length of the column value (not includes itself) (zero possible, -1: special case - NULL col val (no value bytes follow in the NULL case), (int32)
	value of the col (in format indicated by associated format code) (string)00

C: CommandComplete - msgBody is commandTag (which sql command was completed)
	SET (not in list on website, but "observed in the wild"),
	INSERT oid rows (oid=0, rows = #rows inserted),
	SELECT rows (rows = #rows retrieved --> used for SELECT and CREATE TABLE AS commands),
	UPDATE rows (rows = #rows updated),
	DELETE rows (rows = #rows deleted),
	MOVE rows (rows = #rows the cursor's position has been changed by (??)),
	FETCH rows (rows = #rows that have been retrieved from cursor),
	COPY rows (rows = #rows copied --> only on PSQL 8.2 and later)

Z: Ready for query (tags)
	I: idle


1....2....T.....  lolid...@  .  . .  ... .  . .  .  .  .  .  ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
1....2....T.....1 lolid...40 02 . 01 ... 17 . 04 ff ff ff ff ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
differences from wireshark Data.data (diffs at right position)
ff's format code?


from website:
atttypmod int4
atttypmod records type-specific data supplied at table creation time (for example, the maximum length of a varchar column).
It is passed to type-specific input functions and length coercion functions. The value will generally be -1 for types that do not need atttypmod.

typlen int2
For a fixed-size type, typlen is the number of bytes in the internal representation of the type. But for a variable-length type, typlen is negative.
-1 indicates a “varlena” type (one that has a length word), -2 indicates a null-terminated C string.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1....2....T...S..albumid...@...............title...@...............artistid...@...............D..........1....Hello....1D..........2....Hello....2D..........3....lol....3C...SELECT 3.Z....I
1....2....T...S..albumid...@...............title...@...............artistid...@...............D..........1....Hello....1D..........2....Hello....2D..........3....lol....3C...SELECT 3.Z....I

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
insert:

P...).INSERT INTO lol(LolId) VALUES (4)...B............D....P.E...	.....S....
1....2....n....C....INSERT 0 1.Z....I
X....

n: noData indicator
C: CommandComplete

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
See diary for darstellung with Null (12.10.22)

           r   user flufi database flufi client_encoding UTF8 DateStyle ISO TimeZone Europe/Berlin extra_float_digits 2

        P   " SET extra_float_digits = 3   B           E   	    S   

        P   7 SET application_name = 'PostgreSQL JDBC Driver'   B           E   	    S   

        P   ) INSERT INTO lol(LolId) VALUES (3)    B               D      P   E    	     S     

        P    SELECT LolId FROM lol    B               D     P   E    	      S     
        P    SELECT LolId FROM lol   B           D   P E   	     S   



     */
}
