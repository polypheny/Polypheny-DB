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
import org.apache.commons.lang.ArrayUtils;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.*;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.*;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class PGInterfaceQueryHandler{
    String query;
    ChannelHandlerContext ctx;
    PGInterfaceInboundCommunicationHandler communicationHandler;
    private TransactionManager transactionManager;
    int rowsAffected = 0;   //rows affected (changed/deleted/inserted/etc)
    List<List<Object>> rows;

    public PGInterfaceQueryHandler (String query, ChannelHandlerContext ctx, PGInterfaceInboundCommunicationHandler communicationHandler, TransactionManager transactionManager) {
        this.query = query;
        this.ctx = ctx;
        this.communicationHandler = communicationHandler;
        Object obj = new Object();
        this.transactionManager = transactionManager;
    }

    public void start() {
        sendQueryToPolypheny();
    }

    public void sendQueryToPolypheny() {
        String type = ""; //query type according to answer tags
        //get result from polypheny
        Transaction transaction;
        Statement statement = null;
        PolyResult result;
        ArrayList<String[]> data = new ArrayList<>();
        ArrayList<String[]> header = new ArrayList<>();



        try {
            //get transaction letze linie
            transaction = transactionManager.startTransaction("pa", "APP", false, "Index Manager");
            statement = transaction.createStatement();
        }
        catch (UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e) {
            throw new RuntimeException( "Error while starting transaction", e );
        }



        //get algRoot  --> use it in abstract queryProcessor (prepare query) - example from catalogImpl (461-446)
        //for loop zom dor alli catalogTables doregoh? - nei
        Processor sqlProcessor = statement.getTransaction().getProcessor(Catalog.QueryLanguage.SQL);
        Node sqlNode = sqlProcessor.parse(query);   //go gehts fähler: (see diary)
        QueryParameters parameters = new QueryParameters( query, Catalog.SchemaType.RELATIONAL );
        if ( sqlNode.isA( Kind.DDL ) ) {
            result = sqlProcessor.prepareDdl( statement, sqlNode, parameters );
            //TODO(FF): ene try catch block... || evtl no committe (söscht werds ned aazeigt em ui (aso allgemein, wie werds denn aazeigt em ui?)
            // exception: java.lang.RuntimeException: No primary key has been provided!
        } else {
            AlgRoot algRoot = sqlProcessor.translate(
                    statement,
                    sqlProcessor.validate(statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean()).left,
                    new QueryParameters(query, Catalog.SchemaType.RELATIONAL));

            //get PolyResult from AlgRoot - use prepareQuery from abstractQueryProcessor (example from findUsages)
            final QueryProcessor processor = statement.getQueryProcessor();
            result = processor.prepareQuery(algRoot, true);

        }

        //get type information - from crud.java
        //Type of ArrayList was DbColumn, but belongs to webUI (so I don't need it...) --> replaced it with string?
        //ArrayList<String> header = getHeader(result, query);   //descriptor för jedi col befors es resultat get (aahgehni ziile) --> de muesi no hole??
header = getHeader(result);
        //statement.executeUpdate("SELECT empid FROM public.emps");
        
        //get actual result of query in array - from crud.java
        rows = result.getRows(statement, -1);   //-1 as size valid??
        data = computeResultData(rows, header);   //, statement.getTransaction()

        //type = result.getStatementType().name();
        type = result.getStatementType().toString();

        //how to handle reusable queries?? (do i have to safe them/check if it is reusable?)

        //handle result --> depending on query type, prepare answer message accordingly here (flush it)
        sendResultToClient(type, data, header);

    }

    /**
     * gets the information for the header
     * @param result the polyresult the additional information is needed
     * @return a list with array, where:
     *         - array[0] = columnName
     *         - array[1] = columnType
     */
    private ArrayList<String[]> getHeader(PolyResult result) {    //(request = query)
        ArrayList<String[]> header = new ArrayList<>();
        for ( AlgDataTypeField metaData : result.getRowType().getFieldList() ) {
            String columnName = metaData.getName();
            //final String name = metaData.getName();
            String dataType = metaData.getType().getPolyType().getTypeName();   //INTEGER, VARCHAR --> aber ergendwie ohnis (20) em header??
            int precision = metaData.getType().getPrecision();  //sizeVarChar
            boolean nullable = metaData.getType().isNullable() == (ResultSetMetaData.columnNullable == 1);
            //Integer precision = metaData.getType().getPrecision();

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
            //header.add( dbCol );
            header.add(new String[]{columnName, dataType, String.valueOf(precision)});
        }
        return header;
    }

    private ArrayList<String[]> computeResultData(List<List<Object>> rows, ArrayList<String[]> header) {
        //TODO(FF): bruuch ich de header do öberhaupt? (aso em momänt ned... ) --> hanich sache wonich de chönnt/müesst bruuche?
        //ha es paar sache useglöscht... aber wahrschiinli muesmer de no meh lösche...
        ArrayList<String[]> data = new ArrayList<>();

        for ( List<Object> row : rows ) {
            String[] temp = new String[row.size()]; //temp esch au 100 --> vo resultat sälber...
            int counter = 0;
            for ( Object o : row ) {
                if ( o == null ) {
                    temp[counter] = null;
                } else {
                    switch ( header.get( counter )[0] ) {  //TODO(FF): is switch case nessecary?? if yes, get meaningfull header entry (only handling "standard" returns
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
                counter++;  //was macht gnau de counter? (esch etzt 1, chonnt add), rows size = 4
            }
            data.add( temp );
        }


        return data;
    }


    public void sendResultToClient(String type, ArrayList<String[]> data, ArrayList<String[]> header) {
        switch (type) {
            case "INSERT":
                //TODO(FF): actually do the things in polypheny --> track number of changed rows (if easy, doesnt really matter for client so far)
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
                communicationHandler.sendCommandCompleteInsert(rowsAffected);
                communicationHandler.sendReadyForQuery("I");

                break;

            case "CREATE TABLE":
                //TODO(FF) do things in polypheny (?)
                //1....2....n....C....CREATE TABLE.Z....I
                communicationHandler.sendParseBindComplete();
                communicationHandler.sendCommandCompleteCreateTable();
                communicationHandler.sendReadyForQuery("I");

                break;

            case "SELECT" : //also CREATE TABLE AS
                int lol = 4;
                ArrayList<Object[]> valuesPerCol = new ArrayList<Object[]>();

                String fieldName = "";  //get field name from query? momentan no de einzig val em header                                                o
                int objectIDTable = 0;   //int32 --> eig. ObjectID of table (if col can be id'd to table) --> otherwise 0                               o
                int attributeNoCol = 0;    //int16 --> attr.no of col (if col can be id'd to table) --> otherwise 0                                     o
                int objectIDCol = 0;    //int32 --> objectID of parameter datatype (specified in parse message (F), at the end) --> 0=unspecified       o
                int formatCode = 0;     //int16 --> zero(text-inhalt (values)) or one(integer) --> if returned from describe, not yet known = 0         o.
                int typeModifier = -1;  //The value will generally be -1 for types that do not need atttypmod. --> type specific data (supplied at table creation time  o

                int dataTypeSize = 0;   //int16 --> polypheny website typedocumentation aaluege (real=double in polypheny) --> in postgresqlStore shauen welche grösse wie gemappt
                //gibt methode um sql type zu holen dort --> luege wies dbms meta macht (avatica generall interface hauptklasse) --> irgendwas mit negative vals=variable width types
                //For a fixed-size type, typlen is the number of bytes in the internal representation of the type. But for a variable-length type, typlen is negative
                //                                                                                                                                      o.


                if (lol == 3) {   //data.isEmpty() TODO(FF): das useneh, bzw usefende wenn noData etzt gnau gscheckt werd...
                    //noData
                    //communicationHandler.sendNoData();    //should only be sent when frontend sent no data (?)
                    communicationHandler.sendParseBindComplete();
                    communicationHandler.sendReadyForQuery("I");
                }

                else {
                    //data
                    //for loop mache för jedi reihe? --> nocheluege wies gmacht werd em ächte psql met mehrere cols & reihe
                    int numberOfFields = header.size();

                    for (String[] head : header) {

                        fieldName = head[0];

                        //TODO(FF): Implement the rest of the cases
                        switch (head[1]) {
                            case "BIGINT":
                            case "DOUBLE":
                                dataTypeSize = 8;   //8 bytes signed
                                formatCode = 1; //TODO(FF): esch das rechtig? wel es heisst e de doc darstellig vo Integer...
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
                                dataTypeSize = 4;
                                formatCode = 1;
                                break;
                            case "VARCHAR":
                                //dataTypeSize = Integer.parseInt(head[2]); //TODO(FF): wennd varchar längi de type modifier esch, was esch denn dataTypeSize
                                //formatCode = 0;
                                dataTypeSize = 4;
                                formatCode = 1;
                                break;
                            case "SMALLINT":
                                dataTypeSize = 2;
                                formatCode = 1;
                                break;
                            case "TINYINT":
                                dataTypeSize = 1;
                                formatCode = 1;
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
                        //rowDescription
                        //communicationHandler.sendRowDescription(fieldName, objectIDTable, attributeNoCol, objectIDCol, dataTypeSize, typeModifier, formatCode);
                        Object col[] = {fieldName, objectIDTable, attributeNoCol, objectIDCol, dataTypeSize, typeModifier, formatCode};
                        valuesPerCol.add(col);
                    }
                    communicationHandler.sendParseBindComplete();
                    communicationHandler.sendRowDescription(numberOfFields, valuesPerCol);
                    //sendData
                    communicationHandler.sendDataRow(data);

                    rowsAffected = data.size();
                    communicationHandler.sendCommandCompleteSelect(rowsAffected);

                    communicationHandler.sendReadyForQuery("I");
                }


                //SELECT rows (rows = #rows retrieved --> used for SELECT and CREATE TABLE AS commands)
                //1....2....T.....  lolid...@  .  . .  ... .  . .  .  .  .  .  ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
                //1....2....T.....1 lolid...40 02 . 01 ... 17 . 04 ff ff ff ff ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
                break;

            case "DELETE" :
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


     */
}
