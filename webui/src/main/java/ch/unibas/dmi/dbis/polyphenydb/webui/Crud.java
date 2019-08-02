/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.information.Information;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationHtml;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationObserver;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbTable;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Debug;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Index;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Result;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ResultType;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Schema;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SidebarElement;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.TableConstraint;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Uml;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.EditTableRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.QueryRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.UIRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.LocalTransactionHandler;
import com.google.gson.Gson;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;


public abstract class Crud implements InformationObserver {

    static final Logger LOGGER = LoggerFactory.getLogger( CrudPostgres.class );
    final String dbName;
    protected final String URL;
    private final String USER;
    private final String PASS;
    @Getter
    private final int PORT;
    final int PAGESIZE = 4;
    private String driver;
    Gson gson = new Gson();


    /**
     * Constructor
     * @param jdbc jdbc url
     * @param driver driver name
     * @param host host name
     * @param port port
     * @param dbName database name
     * @param user user name
     * @param pass password
     */
     Crud( final String driver, final String jdbc, final String host, final int port, final String dbName, final String user, final String pass ) {
        this.driver = driver;
        this.dbName = dbName;
        //Time zone: https://stackoverflow.com/questions/26515700/mysql-jdbc-driver-5-1-33-time-zone-issue
        this.URL = jdbc + host + ":" + port + "/" + dbName;//"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

        this.USER = user;
        this.PASS = pass;
        this.PORT = port;

        try {
            Class.forName( driver );
        } catch ( ClassNotFoundException e ) {
            LOGGER.error( "Could not load driver class." );
            LOGGER.error( e.getMessage() );
        }
    }


    /**
     * Get an instance of the LocalTransactionHandler
     */
    LocalTransactionHandler getHandler() {
        LocalTransactionHandler handler = null;
        try {
            handler = LocalTransactionHandler.getTransactionHandler( this.driver, this.URL, this.USER, this.PASS );
        } catch ( CatalogConnectionException e ) {
            LOGGER.error( "Could not get TransactionHandler", e );
        }
        return handler;
    }


    /**
     * get the Number of rows in a table
     */
     int getTableSize ( final UIRequest request ) {
        int size = 0;
        String query = "SELECT count(*) FROM " + request.tableId;
        if( request.filter != null) query += " " + filterTable( request.filter );
        LocalTransactionHandler handler = getHandler();
        try ( ResultSet rs = handler.executeSelect( query ) ) {
            rs.next();
            size = rs.getInt( 1 );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            LOGGER.error( e.getMessage() );
        }
        return size;
    }


    /**
     * returns the content of a table
     * with a maximum of PAGESIZE elements
     */
    Result getTable( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;
        LocalTransactionHandler handler = getHandler();

        StringBuilder query = new StringBuilder();
        String where = "";
        if( request.filter != null) where = filterTable( request.filter );
        String orderBy = "";
        if ( request.sortState != null ) orderBy = sortTable( request.sortState );
        query.append( "SELECT * FROM " ).append( request.tableId ).append( where ).append( orderBy ).append( " LIMIT " ).append( PAGESIZE ).append( " OFFSET " ).append( ( Math.max( 0, request.currentPage - 1 )) * PAGESIZE );

        try ( ResultSet rs = handler.executeSelect( query.toString() ) ) {
            result = buildResult( rs, request );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            //result = new Result( e.getMessage() );
            result = new Result( "Could not fetch table " + request.tableId );
            try {
                handler.rollback();
                return result;
            } catch ( CatalogTransactionException ex ) {
                LOGGER.error( "Could not rollback", ex );
            }
        }

        //determine if it is a view or a table
        String[] t = request.tableId.split( "\\." );
        try ( ResultSet rs = handler.getMetaData().getTables( this.dbName, t[0], t[1], null ) ) {
            rs.next();//expecting only one result
            if( rs.getString( 4 ).equals( "TABLE" ) ){
                result.setType( ResultType.TABLE );
            } else {
                result.setType( ResultType.VIEW );
            }
        } catch ( SQLException e ) {
            LOGGER.error( e.toString() );
            result.setError( "Could not retrieve type of Result (table/view)." );
        }

        result.setCurrentPage( request.currentPage ).setTable( request.tableId );
        int tableSize = getTableSize( request );
        result.setHighestPage( (int) Math.ceil( (double) tableSize / PAGESIZE ) );
        return result;
    }


    /**
     * From a ResultSet: build a Result object that the UI can understand
     */
     Result buildResult( final ResultSet rs, final UIRequest request ) {
        ArrayList<String[]> data = new ArrayList<>();
        ArrayList<DbColumn> header = new ArrayList<>();
        Result result;

        try {
            ResultSetMetaData meta = rs.getMetaData();
            int numOfCols = meta.getColumnCount();
            for( int i = 1; i <= numOfCols; i++) {
                String col = meta.getColumnName( i );
                String filter = "";
                if(request.filter != null && request.filter.containsKey( col )){
                    filter = request.filter.get( col );
                }
                SortState sort;
                if( request.sortState != null && request.sortState.containsKey( col )){
                    sort = request.sortState.get( col );
                } else {
                    sort = new SortState();
                }
                // todo: get default value
                header.add( new DbColumn( meta.getColumnName( i ), meta.getColumnTypeName( i ), meta.isNullable( i ) == ResultSetMetaData.columnNullable, meta.getColumnDisplaySize( i ), sort, filter ) );
            }
            while ( rs.next() ) {
                String[] row = new String[numOfCols];
                for( int i = 1; i <= numOfCols; i++) {
                    row[i-1] = rs.getString( i );
                }
                data.add( row );
            }
            result = new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ) ).setInfo( new Debug().setAffectedRows( data.size() ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    abstract ArrayList<SidebarElement> getSchemaTree( final Request req, final Response res );


    /**
     * Get all tables of a schema
     */
    Result getTables( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        LocalTransactionHandler handler = getHandler();
        Result result;
        try ( ResultSet rs = handler.getMetaData().getTables( this.dbName, request.schema, null, new String[]{"TABLE"} )) {
            ArrayList<String> tables = new ArrayList<>();
            while ( rs.next() ) {
                tables.add( rs.getString( 3 ) );
            }
            result = new Result( new Debug().setAffectedRows( tables.size() ) ).setTables( tables );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Drop or truncate a table
     */
    Result dropTruncateTable( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Result result;
        StringBuilder query = new StringBuilder();
        if ( request.action.toLowerCase().equals( "drop" ) ) {
            query.append( "DROP TABLE " );
        } else if ( request.action.toLowerCase().equals( "truncate" ) ) {
            query.append( "TRUNCATE " );
        }
        query.append( request.schema ).append( "." ).append( request.table );
        LocalTransactionHandler handler = getHandler();
        try {
            int a = handler.executeUpdate( query.toString() );
            result = new Result( new Debug().setAffectedRows( 1 ).setGeneratedQuery( query.toString() ) );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
        }
        return result;
    }


    abstract Result createTable( final Request req, final Response res );


    /**
     * insert data into a table
     */
    Result insertIntoTable( final Request req, final Response res ) {
        int rowsAffected = 0;
        Result result;
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        StringJoiner cols = new StringJoiner( ",", "(", ")" );
        StringBuilder query = new StringBuilder();
        query.append( "INSERT INTO " ).append( request.tableId );
        StringJoiner values = new StringJoiner( ",", "(", ")" );
        for ( Map.Entry<String, String> entry : request.data.entrySet() ) {
            cols.add( entry.getKey() );
            String value = entry.getValue();
            if( value == null ){
                value = "NULL";
            }
            //todo default value
            else if( ! NumberUtils.isNumber( value )) {
                value = "'"+value+"'";
            }
            values.add( value );
        }
        query.append( cols.toString() );
        query.append( " VALUES " ).append( values.toString() );

        LocalTransactionHandler handler = getHandler();
        try {
            rowsAffected = handler.executeUpdate( query.toString() );
            result = new Result( new Debug().setAffectedRows( rowsAffected ).setGeneratedQuery( query.toString() ) );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
        }
        return result;
    }


    protected abstract String filterTable( final Map<String, String> filter );


    /**
     * Generates the ORDER BY clause of a query if a sorted column is requested by the UI
     */
    protected String sortTable( final Map<String, SortState> sorting ) {
        StringJoiner joiner = new StringJoiner( ",", " ORDER BY ", "" );
        int counter = 0;
        for ( Map.Entry<String, SortState> entry : sorting.entrySet() ) {
            if ( entry.getValue().sorting ) {
                joiner.add( entry.getKey() + " " + entry.getValue().direction );
                counter++;
            }
        }
        String out = "";
        if ( counter > 0 ) out = joiner.toString();
        return out;
    }


    /**
     * Run any query coming from the SQL console
     */
    ArrayList<Result> anyQuery( final Request req, final Response res ) {
        QueryRequest request = this.gson.fromJson( req.body(), QueryRequest.class );
        ArrayList<Result> results = new ArrayList<>();
        LocalTransactionHandler handler = getHandler();
        boolean autoCommit = true;

        //todo make it possible to use pagination

        //No autoCommit if the query has commits.
        //ignore case: from: https://alvinalexander.com/blog/post/java/java-how-case-insensitive-search-string-matches-method
        Pattern p = Pattern.compile(".*(COMMIT|ROLLBACK).*", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher m = p.matcher(request.query);
        if( m.matches() ) {
            autoCommit = false;
        }

        long executionTime = 0;
        long temp = 0;
        Pattern semicolon = Pattern.compile( ";[\\s]*$", Pattern.MULTILINE );//find all semicolons at the end of a line and split there
        String[] queries = semicolon.split( request.query );
        for ( String query: queries ) {
            Result result;
            if( Pattern.matches( "(?si:[\\s]*COMMIT.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    handler.commit();
                    executionTime += System.nanoTime() - temp;
                    results.add( new Result( new Debug().setGeneratedQuery( query )) );
                } catch ( CatalogTransactionException e ) {
                    executionTime += System.nanoTime() - temp;
                    LOGGER.error( e.toString() );
                }
            } else if( Pattern.matches( "(?si:[\\s]*ROLLBACK.*)", query ) ) {
                try {
                    temp = System.nanoTime();
                    handler.rollback();
                    executionTime += System.nanoTime() - temp;
                    results.add( new Result( new Debug().setGeneratedQuery( query )) );
                } catch ( CatalogTransactionException  e ) {
                    executionTime += System.nanoTime() - temp;
                    LOGGER.error( e.toString() );
                }
            } else if( Pattern.matches( "(?si:^[\\s]*SELECT.*)", query ) ) {
                //Add limit if not specified
                if( ! Pattern.matches(".*?(?si:limit)[\\s\\S]*", query )){
                    query = query + " LIMIT " + this.PAGESIZE;
                }
                //decrease limit if it is too large
                else{
                    Pattern pattern = Pattern.compile( "(.*?LIMIT[\\s+])(\\d+)" );
                    Matcher limitMatcher = pattern.matcher( query );
                    if( limitMatcher.find() ){
                        int limit = Integer.parseInt( limitMatcher.group(2) );
                        if( limit > this.PAGESIZE ){
                            //see https://stackoverflow.com/questions/38296673/replace-group-1-of-java-regex-with-out-replacing-the-entire-regex?rq=1
                            query = limitMatcher.replaceFirst("$1 " + this.PAGESIZE);
                        }
                    }
                }
                try {
                    temp = System.nanoTime();
                    ResultSet rs = handler.executeSelect( query );
                    executionTime += System.nanoTime() - temp;
                    result = buildResult( rs, request ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                    if( autoCommit ) handler.commit();
                    rs.close();
                } catch ( SQLException | CatalogTransactionException e ) {
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                }
            } else {
                try {
                    temp = System.nanoTime();
                    int numOfRows = handler.executeUpdate( query );
                    executionTime += System.nanoTime() - temp;
                    result = new Result( new Debug().setAffectedRows( numOfRows ).setGeneratedQuery( query ) );
                    results.add( result );
                    if( autoCommit ) handler.commit();
                } catch ( SQLException | CatalogTransactionException e ) {
                    executionTime += System.nanoTime() - temp;
                    result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                }
            }
        }

        if( request.analyze ){
            InformationManager queryAnalyzer = InformationManager.getInstance( UUID.randomUUID().toString() ).observe( this );
            InformationPage p1 = new InformationPage( "p1", "Query analysis", "Analysis of the query." );
            InformationGroup g1 = new InformationGroup( "Execution time", "p1" );
            InformationHtml html;
            if( executionTime < 1e4 ) {
                html = new InformationHtml( "exec_time", "Execution time", String.format("Execution time: %d nanoseconds", executionTime) );
            } else {
                double execTime = Math.round( executionTime / 1e6 ) + (executionTime % 1e6)/1e6;
                html = new InformationHtml( "exec_time", "Execution time", String.format("Execution time: %.2f milliseconds", execTime) );
            }
            //todo convert to seconds / minutes if needed
            queryAnalyzer.addPage( p1 );
            queryAnalyzer.addGroup( g1 );
            queryAnalyzer.registerInformation( html );
        }

        return results;
    }


    /**
     * delete a row from a table
     * the row is determined by the value of every column in that row (conjunction)
     * the transaction is being rolled back, if more that one row would be deleted
     */
    Result deleteRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;
        StringBuilder builder = new StringBuilder();
        builder.append( "DELETE FROM " ).append( request.tableId ).append( " WHERE " );
        StringJoiner joiner = new StringJoiner( " AND ", "", "" );
        for ( Entry<String, String> entry : request.data.entrySet() ) {
            String condition = "";
            if ( entry.getValue() == null) {
                //todo fix: doesn't work for integers
                condition = String.format( "%s IS NULL", entry.getKey() );
            } else {
                condition = String.format( "%s = '%s'", entry.getKey(), entry.getValue() );
            }
            joiner.add( condition );
        }
        builder.append( joiner.toString() );

        LocalTransactionHandler handler = getHandler();
        try {
            int numOfRows = handler.executeUpdate( builder.toString() );
            //only commit if one row is deleted
            if ( numOfRows == 1 ) {
                handler.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                handler.rollback();
                result = new Result( "Attempt to delete " + numOfRows + " rows was blocked." ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
        }
        return result;
    }


    Result updateRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;
        StringBuilder builder = new StringBuilder();
        builder.append( "UPDATE " ).append( request.tableId ).append( " SET " );
        StringJoiner setStatements = new StringJoiner( ",", "", "" );
        for ( Entry<String, String> entry : request.data.entrySet() ) {
            //todo default value
            if ( entry.getValue() == null ) {
                setStatements.add( String.format( "%s = NULL", entry.getKey() ) );
            } else if ( NumberUtils.isNumber( entry.getValue() ) ) {
                setStatements.add( String.format( "%s = %s", entry.getKey(), entry.getValue() ) );
            } else {
                setStatements.add( String.format( "%s = '%s'", entry.getKey(), entry.getValue() ) );
            }
        }
        builder.append( setStatements.toString() );

        StringJoiner where = new StringJoiner( " AND ", "", "" );
        for ( Entry<String, String> entry : request.filter.entrySet() ) {
            where.add( String.format( "%s = '%s'", entry.getKey(), entry.getValue() ) );
        }
        builder.append( " WHERE " ).append( where.toString() );

        LocalTransactionHandler handler = getHandler();
        try {
            int numOfRows = handler.executeUpdate( builder.toString() );

            if ( numOfRows == 1 ) {
                handler.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                handler.rollback();
                result = new Result( "Attempt to update " + numOfRows + " rows was blocked." ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
        }
        return result;
    }


    /**
     * Get the columns of a table
     */
    Result getColumns( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        LocalTransactionHandler handler = getHandler();
        Result result;

        ArrayList<String> primaryColumns = new ArrayList<>();
        String[] t = request.tableId.split( "\\." );
        ArrayList<DbColumn> cols = new ArrayList<>();

        try ( ResultSet rs = handler.getMetaData().getPrimaryKeys( this.dbName, t[0], t[1] )) {
            while( rs.next() ){
                primaryColumns.add( rs.getString( 4 ) );
            }
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }

        handler = getHandler();
        try ( ResultSet rs = handler.getMetaData().getColumns( this.dbName, t[0], t[1], "" ) ) {
            while( rs.next() ){
                cols.add( new DbColumn( rs.getString( 4 ), rs.getString( 6 ), rs.getInt( 11 ) == 1, rs.getInt( 7 ), primaryColumns.contains( rs.getString( 4 ) ), "" ) );
            }
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }

        result = new Result( cols.toArray( new DbColumn[0] ), null );

        return result;
    }


    abstract Result updateColumn( final Request req, final Response res );


    abstract Result addColumn( final Request req, final Response res );


    abstract Result dropColumn( final Request req, final Response res );


    /**
     * Get constraints of a table
     */
    Result getConstraints( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        String[] t = request.tableId.split( "\\." );
        Result result;
        LocalTransactionHandler handler = getHandler();
        ArrayList<TableConstraint> constraints = new ArrayList<>();
        Map<String, ArrayList<String>> temp = new HashMap<>();

        //get primary keys
        try ( ResultSet rs = handler.getMetaData().getPrimaryKeys( this.dbName, t[0], t[1] )) {
            while ( rs.next() ){
                if( ! temp.containsKey( rs.getString( 6 ))){
                    temp.put( rs.getString( 6 ), new ArrayList<>() );
                }
                temp.get( rs.getString( 6 ) ).add( rs.getString( 4 ) );
            }
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }

        for ( Map.Entry<String, ArrayList<String>> entry: temp.entrySet()) {
            constraints.add( new TableConstraint( entry.getKey(), "PRIMARY KEY", entry.getValue() ));
        }

        //get foreign keys
        temp.clear();
        handler = getHandler();
        try ( ResultSet rs = handler.getMetaData().getImportedKeys( this.dbName, t[0], t[1] )) {
            while ( rs.next() ){
                if( ! temp.containsKey( rs.getString( 12 ))){
                    temp.put( rs.getString( 12 ), new ArrayList<>() );
                }
                temp.get( rs.getString( 12 ) ).add( rs.getString( 8 ) );
            }
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }

        for ( Map.Entry<String, ArrayList<String>> entry: temp.entrySet()) {
            constraints.add( new TableConstraint( entry.getKey(), "FOREIGN KEY", entry.getValue() ));
        }

        DbColumn[] header = { new DbColumn( "constraint name" ), new DbColumn( "constraint type" ), new DbColumn( "columns" ) };
        ArrayList<String[]> data = new ArrayList<>();
        constraints.forEach( (c) -> {
            data.add( c.asRow() );
        } );

        result = new Result( header, data.toArray( new String[0][2] ));
        return result;
    }


    abstract Result dropConstraint( final Request req, final Response res );


    abstract Result addPrimaryKey( final Request req, final Response res );


    /**
     * Get indexes of a table
     */
    Result getIndexes( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Result result;
        LocalTransactionHandler handler = getHandler();

        try {
            ResultSet indexInfo = handler.getMetaData().getIndexInfo( this.dbName, request.schema, request.table, false, true );
            DbColumn[] header = {new DbColumn( "name" ), new DbColumn( "columns" )};
            Map<String, StringJoiner> indexes = new HashMap<>();
            ArrayList<String[]> data = new ArrayList<>();
            while ( indexInfo.next() ) {
                String name = indexInfo.getString( 6 );
                String col = indexInfo.getString( 9 );
                if( ! indexes.containsKey( name )){
                    indexes.put( name, new StringJoiner( ", " ) );
                }
                indexes.get( name ).add( col );
            }
            for ( Map.Entry<String, StringJoiner> entry: indexes.entrySet() ){
                String[] insert = new String[2];
                insert[0] = entry.getKey();
                insert[1] = entry.getValue().toString();
                data.add( insert );
            }
            result = new Result( header, data.toArray( new String[0][2] ) );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Drop an index of a table
     */
    Result dropIndex( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        LocalTransactionHandler handler = getHandler();
        String query = String.format( "DROP INDEX %s.%s", request.schema, request.action );
        Result result;
        try {
            int a = handler.executeUpdate( query );
            handler.commit();
            result = new Result( new Debug().setGeneratedQuery( query ).setAffectedRows( a ) );
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Create an index for a table
     */
    Result createIndex( final Request req, final Response res ) {
        Index index = this.gson.fromJson( req.body(), Index.class );
        LocalTransactionHandler handler = getHandler();
        Result result;
        try {
            int a = handler.executeUpdate( index.create() );
            handler.commit();
            result = new Result( new Debug().setAffectedRows( a ) );
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Get the needed information for the uml view:
     * Foreign keys
     * Tables with its columns
     */
    Uml getUml ( final Request req, final Response res ) {
        LocalTransactionHandler handler = getHandler();
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        ArrayList<ForeignKey> fKeys = new ArrayList<>();
        try ( ResultSet rs = handler.getMetaData().getImportedKeys( this.dbName, request.schema, null ) ) {
            while ( rs.next() ){
                fKeys.add( ForeignKey.builder()
                    .pkTableSchema( rs.getString( 2 ))
                    .pkTableName( rs.getString( 3 ))
                    .pkColumnName( rs.getString( 4 ))
                    .fkTableSchema( rs.getString( 6 ))
                    .fkTableName( rs.getString( 7 ))
                    .fkColumnName( rs.getString( 8 ))
                    .fkName( rs.getString( 12 ))
                    .pkName( rs.getString( 13 ))
                    .build());
            }
        } catch ( SQLException e ) {
            LOGGER.error( "Could not fetch foreign keys of the schema " + request.schema, e );
        }

        //get Tables with its columns
        ArrayList<DbTable> tables = new ArrayList<>();
        String[] types = {"TABLE"};
        try ( ResultSet rs = handler.getMetaData().getTables( this.dbName, request.schema, null, types )) {
            while ( rs.next() ){
                DbTable table = new DbTable( rs.getString( 3 ), request.schema );
                ResultSet rs2 = handler.getMetaData().getColumns( this.dbName, request.schema, rs.getString( 3 ), null );
                while ( rs2.next() ){
                    table.addColumn( new DbColumn( rs2.getString( 4 )));
                }

                //get primary key with its columns
                ResultSet pkSet = handler.getMetaData().getPrimaryKeys( this.dbName, request.schema,  rs.getString(3));
                while( pkSet.next() ){
                    table.addPrimaryKeyField( pkSet.getString( 4 ) );
                }
                pkSet.close();

                //get unique columns using indexes, see https://stackoverflow.com/questions/1674223/find-a-database-tables-unique-constraint
                ResultSet uniqueCols = handler.getMetaData().getIndexInfo( this.dbName, request.schema, rs.getString( 3 ), true, true );
                while ( uniqueCols.next() ){
                    table.addUniqueColumn( uniqueCols.getString( 9 ) );
                }
                uniqueCols.close();

                tables.add( table );
            }
        } catch ( SQLException e ) {
            LOGGER.error( "Could not fetch tables of the schema " + request.schema );
        }

        return new Uml( tables, fKeys );
    }


    /**
     * Add foreign key
     */
    Result addForeignKey( final Request req, final Response res ) {
        ForeignKey foreignKey = this.gson.fromJson( req.body(), ForeignKey.class );
        LocalTransactionHandler handler = getHandler();
        Result result;
        try {
            foreignKey.create( handler );
            result = new Result( new Debug().setAffectedRows( 1 ) );
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Prototype to execute Relational Algebra
     */
    // todo implement execution of relational algebra
    Result executeRelAlg ( final Request req, final Response res ) {
        LOGGER.info( req.body() );
        return new Result( "RelAlg execution not implemented yet." );
    }


    /**
     * Create or drop a schema
     */
    Result schemaRequest ( final Request req, final Response res ) {
        Schema schema = this.gson.fromJson( req.body(), Schema.class );
        return schema.executeCreateOrDrop( getHandler() );
    }


    /**
     * Get all supported data types of the DBMS.
     */
    public Result getTypeInfo ( final Request req, final Response res ) {
        LocalTransactionHandler handler = getHandler();
        ArrayList<String[]> data = new ArrayList<>();
        Result result;
        try ( ResultSet rs = handler.getMetaData().getTypeInfo() ) {
            while( rs.next() ){
                // ignore types that are not relevant
                if( rs.getInt( 2 ) < -500 || rs.getInt( 2 ) > 500 ) continue;
                String[] row = new String[1];
                for( int i = 1; i<=18; i++){
                    row[0] = rs.getString( 1 );
                }
                data.add( row );
            }
            DbColumn[] header = { new DbColumn( "TYPE_NAME" ) };
            result = new Result( header, data.toArray( new String[0][1]) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
            e.printStackTrace();
        }
        return result;
    }


    /**
     * Send updates to the UI if Information objects in the query analyzer change.
     */
    @Override
    public void observeInfos( final Information info ) {
        try {
            CrudWebSocket.broadcast( info.asJson() );
        } catch ( IOException e ) {
            LOGGER.error( e.getMessage() );
        }
    }


    /**
     * Send an updated pageList of the query analyzer to the UI.
     */
    @Override
    public void observePageList( final String analyzerId, final InformationPage[] pages ) {
        ArrayList<SidebarElement> nodes = new ArrayList<>();
        int counter = 0;
        for( InformationPage page: pages ){
            nodes.add( new SidebarElement( page.getId(), page.getName(), analyzerId + "/", page.getIcon() ) );
            counter++;
        }
        InformationPage logicalPlan = InformationManager.getInstance().getPage( "informationPageLogicalQueryPlan" );
        if ( logicalPlan != null ) {
            nodes.add( new SidebarElement( logicalPlan.getId(), logicalPlan.getName(), "0/", logicalPlan.getIcon() ) );
        }
        InformationPage physicalPlan = InformationManager.getInstance().getPage( "informationPagePhysicalQueryPlan" );
        if ( physicalPlan != null ) {
            nodes.add( new SidebarElement( physicalPlan.getId(), physicalPlan.getName(), "0/", physicalPlan.getIcon() ) );
        }
        try{
            CrudWebSocket.sendPageList( this.gson.toJson( nodes.toArray( new SidebarElement[0] ) ) );
        } catch ( IOException e ) {
            LOGGER.error( e.getMessage() );
        }
    }


    /**
     * Get the content of an InformationPage of a query analyzer.
     */
    public String getAnalyzerPage( final Request req, final Response res ) {
        String[] params = this.gson.fromJson( req.body(), String[].class );
        return InformationManager.getInstance( params[0] ).getPage( params[1] ).asJson();
    }


    /**
     * Close a query analyzer if not needed anymore.
     */
    public String closeAnalyzer( final Request req, final Response res ) {
        String id = req.body();
        InformationManager.close( id );
        return "";
    }

}
