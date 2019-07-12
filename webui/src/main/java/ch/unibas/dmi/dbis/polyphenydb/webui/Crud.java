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
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.ConstraintRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbTable;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Debug;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.EditTableRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Index;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.QueryRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Result;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ResultType;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.SchemaRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SidebarElement;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.UIRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.ColumnRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Uml;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.LocalTransactionHandler;
import com.google.gson.Gson;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;


/**
 * Create, read, update and delete elements from a database
 * contains only demo data so far
 */
class Crud implements InformationObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger( Crud.class );
    private String driver = "jdbc:postgresql://";
    private final String dbName;
    private final String URL;
    private final String USER;
    private final String PASS;
    private final int PAGESIZE = 4;
    private Gson gson = new Gson();


    /**
     * @param args from command line: "host port database user password"
     */
    Crud( final String[] args ) {

        if( args.length < 4 ) {
            LOGGER.error( "Missing command-line arguments. Please provied the following information:\n"
                    + "java Server <host> <port> <database> <user> <password>\n"
                    + "e.g. java Server localhost 8080 myDatabase root secret" );
            System.exit( 1 );
        }
        String host = args[0];
        int port = Integer.parseInt( args[1] );
        this.dbName = args[2];
        this.USER = args[3];
        String pass = "";
        if ( args.length > 4 ) {
            pass = args[4];
        }
        this.PASS = pass;

        try {
            //Class.forName( "com.mysql.cj.jdbc.Driver" );
            Class.forName( "org.postgresql.Driver" );
        } catch ( ClassNotFoundException e ) {
            LOGGER.error( "Could not load driver class." );
            LOGGER.error( e.getMessage() );
        }

        //Time zone: https://stackoverflow.com/questions/26515700/mysql-jdbc-driver-5-1-33-time-zone-issue
        this.URL = driver + host + ":" + port + "/" + dbName;//"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    }


    /**
     * Get an instance of the LocalTransactionHandler
     */
    private LocalTransactionHandler getHandler() {
        LocalTransactionHandler handler = null;
        try {
            handler = LocalTransactionHandler.getTransactionHandler( this.URL, this.USER, this.PASS );
        } catch ( CatalogConnectionException e ) {
            LOGGER.error( "Could not get TransactionHandler", e );
        }
        return handler;
    }


    /**
     * get the Number of rows in a table
     */
    private Integer getTableSize ( final String tableName ) {
        Integer size = null;
        String query = "SELECT count(*) FROM " + tableName;
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
        query.append( "SELECT * FROM " ).append( request.tableId ).append( where ).append( orderBy ).append( " LIMIT " ).append( PAGESIZE ).append( " OFFSET " ).append( (request.currentPage - 1) * PAGESIZE );

        try ( ResultSet rs = handler.executeSelect( query.toString() ) ) {
            result = buildResult( rs, request );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
            try {
                handler.rollback();
                return new Result( e.getMessage() );
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
        Integer tableSize = getTableSize( request.tableId );
        if( tableSize == null ){
            return new Result( String.format( "The table %s does not exist.", t[1] ));
        }
        result.setHighestPage( (int) Math.ceil( (double) tableSize / PAGESIZE ) );
        return result;
    }


    /**
     * From a ResultSet: build a Result object that the UI can understand
     */
    private Result buildResult( final ResultSet rs, final UIRequest request ) {
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


    /**
     * returns a Tree (in json format) with the Tables of a Database
     */
    ArrayList<SidebarElement> getSchemaTree ( final Request req, final Response res ) {
        SchemaRequest request = this.gson.fromJson( req.body(), SchemaRequest.class );
        ArrayList<SidebarElement> result = new ArrayList<>();
        LocalTransactionHandler handler = getHandler();

        if( request.depth < 1 ){
            LOGGER.error( "Trying to fetch a schemaTree with depth < 1" );
            return new ArrayList<>();
        }

        try ( ResultSet schemas = handler.getMetaData().getSchemas() ) {
            while ( schemas.next() ){
                String schema = schemas.getString( 1 );
                if( schema.equals( "pg_catalog" ) || schema.equals( "information_schema" )) continue;
                SidebarElement schemaTree = new SidebarElement( schema, schema, "", "cui-layers" );

                if( request.depth > 1 ){
                    ResultSet tablesRs = handler.getMetaData().getTables( this.dbName, schema, null, null );
                    ArrayList<SidebarElement> tables = new ArrayList<>();
                    ArrayList<SidebarElement> views = new ArrayList<>();
                    while ( tablesRs.next() ){
                        String tableName = tablesRs.getString( 3 );
                        if( tablesRs.getString( 4 ).equals("TABLE") ){
                            tables.add( new SidebarElement( schema + "." + tableName, tableName, request.routerLinkRoot, "fa fa-table" ));
                        } else if ( request.views && tablesRs.getString( 4 ).equals("VIEW") ){
                            views.add( new SidebarElement( schema + "." + tableName, tableName, request.routerLinkRoot, "fa fa-table" ));
                        }
                    }
                    schemaTree.addChild( new SidebarElement( schema + ".tables", "tables", request.routerLinkRoot, "fa fa-table" ).addChildren( tables ).setRouterLink( "" ) );
                    if( request.views ) {
                        schemaTree.addChild( new SidebarElement( schema + ".views", "views", request.routerLinkRoot, "icon-eye" ).addChildren( views ).setRouterLink( "" ) );
                    }
                    tablesRs.close();
                }
                result.add( schemaTree );
            }
        } catch ( SQLException e ) {
            LOGGER.error( e.getMessage() );
        }

        return result;
    }


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


    /**
     * Create a new table
     */
    Result createTable( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        StringBuilder query = new StringBuilder();
        StringJoiner colJoiner = new StringJoiner( "," );
        query.append( "CREATE TABLE " ).append( request.schema ).append( "." ).append( request.table ).append( "(" );
        StringBuilder colBuilder;
        Result result;
        StringJoiner primaryKeys = new StringJoiner( ",", "PRIMARY KEY (", ")" );
        int primaryCounter = 0;
        for ( DbColumn col : request.columns ) {
            colBuilder = new StringBuilder();
            colBuilder.append( col.name ).append( " " ).append( col.dataType);
            if ( col.maxLength != null ) {
                colBuilder.append( String.format( "(%d)", col.maxLength ) );
            }
            if ( !col.nullable ) {
                colBuilder.append( " NOT NULL" );
            }
            if( col.defaultValue != null ) {
                switch ( col.dataType ) {
                    case "int8":
                    case "int4":
                        int a = Integer.parseInt( col.defaultValue );
                        colBuilder.append( " DEFAULT " ).append( a );
                        break;
                    case "varchar":
                        colBuilder.append( String.format( " DEFAULT '%s'", col.defaultValue ) );
                        break;
                    default:
                        //varchar, timestamptz, bool
                        colBuilder.append( " DEFAULT " ).append( col.defaultValue );
                }
            }
            if ( col.primary ) {
                primaryKeys.add( col.name );
                primaryCounter++;
            }
            colJoiner.add( colBuilder.toString() );
        }
        if ( primaryCounter > 0 ) {
            colJoiner.add( primaryKeys.toString() );
        }
        query.append( colJoiner.toString() );
        query.append( ")" );
        LocalTransactionHandler handler = getHandler();
        try {
            int a = handler.executeUpdate( query.toString() );
            result = new Result( new Debug().setGeneratedQuery( query.toString() ).setAffectedRows( a ) );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
        }
        return result;
    }


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


    /**
     * Filter a table with a keyword
     * Show only entries where the value of that column starts with the keyword
     *
     * @return the generated condition for the query
     */
    private String filterTable ( Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", " WHERE ", "" );
        int counter = 0;
        for ( Map.Entry<String, String> entry : filter.entrySet() ) {
            if ( ! entry.getValue().equals( "" )) {
                joiner.add( entry.getKey() + "::TEXT LIKE '" + entry.getValue() + "%'"  );//:TEXT to cast number to text if necessary (see https://stackoverflow.com/questions/1684291/sql-like-condition-to-check-for-integer#answer-40537672)
                counter++;
            }
        }
        String out = "";
        if( counter > 0 ) out = joiner.toString();
        return out;
    }


    /**
     * Generates the ORDER BY clause of a query if a sorted column is requested by the UI
     */
    private String sortTable ( Map<String, SortState> sorting) {
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
     * Run any query coming form the SQL console
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

        //query inspired from: https://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql

        String[] t = request.tableId.split( "\\." );
        String query = "SELECT column_name, is_nullable, udt_name, character_maximum_length, column_default, "
            + "constraint_type, constraint_name "
            + "FROM( "
            + "select DISTINCT ON (col.column_name) col.column_name, col.is_nullable, col.udt_name, col.character_maximum_length, col.column_default, "
            + "tc.constraint_type, kc.constraint_name, col.ordinal_position "
            + "FROM information_schema.columns col "
            + "LEFT JOIN information_schema.key_column_usage AS kc "
            + "ON col.column_name = kc.column_name "
            + "LEFT JOIN information_schema.table_constraints tc "
            + "ON tc.constraint_name = kc.constraint_name "
            + String.format( "WHERE col.table_schema = '%s' ", t[0])
            + String.format( "AND col.table_name = '%s' ", t[1])
            + "AND (tc.constraint_type = 'PRIMARY KEY' OR tc.constraint_type IS NULL)"
            + ") AS q1 "
            + "ORDER BY ordinal_position ASC";
        //todo use prepared statement

        try ( ResultSet rs = handler.executeSelect( query ) ) {
            ArrayList<DbColumn> cols = new ArrayList<>();
            while ( rs.next() ) {
                boolean isPrimary = false;
                if( rs.getString( "constraint_type" ) != null ){
                    isPrimary = rs.getString( "constraint_type" ).equals( "PRIMARY KEY" );
                }
                //getObject, so you get null and not 0 if the field is NULL
                cols.add( new DbColumn( rs.getString( "column_name" ), rs.getString( "udt_name" ), rs.getBoolean( "is_nullable" ), (Integer) rs.getObject("character_maximum_length"), isPrimary, rs.getString( "column_default" ) ) );
            }
            result = new Result( cols.toArray( new DbColumn[0] ), null );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.toString() );
            LOGGER.error( e.toString() );
        }
        return result;
    }


    /**
     * Update a column of a table
     */
    Result updateColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        DbColumn oldColumn = request.oldColumn;
        DbColumn newColumn = request.newColumn;
        Result result;
        ArrayList<String> queries = new ArrayList<>();
        StringBuilder sBuilder = new StringBuilder();
        LocalTransactionHandler handler = getHandler();

        //rename column if needed
        if ( !oldColumn.name.equals( newColumn.name ) ) {
            String query = String.format( "ALTER TABLE %s RENAME COLUMN %s TO %s", request.tableId, oldColumn.name, newColumn.name );
            queries.add( query );
        }

        //change type + length
        //todo cast if needed
        if ( !oldColumn.dataType.equals( newColumn.dataType ) || !Objects.equals( oldColumn.maxLength, newColumn.maxLength ) ) {
            if ( newColumn.maxLength != null ) {
                String query = String.format( "ALTER TABLE %s ALTER COLUMN %s TYPE %s(%s) USING %s::%s;", request.tableId, newColumn.name, newColumn.dataType, newColumn.maxLength, newColumn.name, newColumn.dataType );
                queries.add( query );
            } else {
                //todo drop maxlength if requested
                String query = String.format( "ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s;", request.tableId, newColumn.name, newColumn.dataType, newColumn.name, newColumn.dataType );
                queries.add( query );
            }
        }

        //set/drop nullable
        if ( oldColumn.nullable != newColumn.nullable ) {
            String nullable = "SET";
            if ( newColumn.nullable ) {
                nullable = "DROP";
            }
            String query = "ALTER TABLE " + request.tableId + " ALTER COLUMN " + newColumn.name + " " + nullable + " NOT NULL";
            queries.add( query );
        }

        //change default value
        if ( oldColumn.defaultValue == null || newColumn.defaultValue == null || !oldColumn.defaultValue.equals( newColumn.defaultValue ) ){
            String query;
            if( newColumn.defaultValue == null ){
                query = String.format( "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", request.tableId, newColumn.name );
            }
            else{
                query = String.format( "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT ", request.tableId, newColumn.name );
                switch ( newColumn.dataType ) {
                    case "int8":
                    case "int4":
                        int a = Integer.parseInt( request.newColumn.defaultValue );
                        query = query + a;
                        break;
                    case "varchar":
                        query = query + String.format( "'%s'", request.newColumn.defaultValue );
                        break;
                    default:
                        //varchar, timestamptz, bool
                        query = query + request.newColumn.defaultValue;
                }
            }
            queries.add( query );
        }

        result = new Result( new Debug().setAffectedRows( 1 ).setGeneratedQuery( queries.toString() ) );
        try{
            for ( String query : queries ){
                handler.executeUpdate( query );
                sBuilder.append( query );
            }
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.toString() ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() ) );
            try {
                handler.rollback();
            } catch ( CatalogTransactionException  e2 ) {
                result = new Result( e2.toString() ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( sBuilder.toString() ) );
            }
        }

        return result;
    }


    /**
     * Add a column to an existing table
     */
    Result addColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        LocalTransactionHandler handler = getHandler();
        String query = String.format( "ALTER TABLE %s ADD COLUMN %s %s", request.tableId, request.newColumn.name, request.newColumn.dataType );
        if ( request.newColumn.maxLength != null ) {
            query = query + String.format( "(%d)", request.newColumn.maxLength );
        }
        if ( !request.newColumn.nullable ) {
            query = query + " NOT NULL";
        }
        if ( request.newColumn.defaultValue != null ){
            switch ( request.newColumn.dataType ) {
                case "int8":
                case "int4":
                    int a = Integer.parseInt( request.newColumn.defaultValue );
                    query = query + " DEFAULT "+a;
                    break;
                case "varchar":
                    query = query + String.format( " DEFAULT '%s'", request.newColumn.defaultValue );
                    break;
                default:
                    //varchar, timestamptz, bool
                    query = query + " DEFAULT " + request.newColumn.defaultValue;
            }
        }
        Result result;
        try {
            int affectedRows = handler.executeUpdate( query );
            handler.commit();
            result = new Result( new Debug().setAffectedRows( affectedRows ).setGeneratedQuery( query ) );
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Delete a column of a table
     */
    Result dropColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        LocalTransactionHandler handler = getHandler();
        Result result;
        String query = String.format( "ALTER TABLE %s DROP COLUMN %s", request.tableId, request.oldColumn.name );
        try {
            int affectedRows = handler.executeUpdate( query );
            handler.commit();
            result = new Result( new Debug().setAffectedRows( affectedRows ) );
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.toString() );
        }
        return result;
    }


    /**
     * Get constraints of a table
     */
    Result getConstraints( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        String[] t = request.tableId.split( "\\." );
        Result result;
        LocalTransactionHandler handler = getHandler();
        String query = "SELECT constraint_name, constraint_type, is_deferrable, initially_deferred "
            + "FROM information_schema.table_constraints tc "
            + String.format("WHERE table_schema = '%s' AND table_name = '%s' ", t[0], t[1])
            + "AND constraint_name NOT LIKE '%not_null'";
        //todo use prepared statement
        try ( ResultSet rs = handler.executeSelect( query ) ) {
            result = buildResult( rs, request );
            handler.commit();
        } catch ( SQLException | CatalogTransactionException e ) {
            result = new Result( e.getMessage() );
        }
        return result;
    }


    /**
     * Drop constraint of a table
     */
    Result dropConstraint ( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
        LocalTransactionHandler handler= getHandler();
        String query = String.format( "ALTER TABLE %s DROP CONSTRAINT %s;", request.table, request.constraint.name );
        Result result;
        try{
            int rows = handler.executeUpdate( query );
            handler.commit();
            result = new Result( new Debug().setAffectedRows( rows ) );
        } catch ( SQLException | CatalogTransactionException e ){
            result = new Result( e.getMessage() );
        }
        return result;
    }

    /**
     * Add a primary key to a table
     */
    Result addPrimaryKey ( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
        LocalTransactionHandler handler = getHandler();
        Result result;
        if( request.constraint.columns.length > 0 ){
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for( String s : request.constraint.columns ){
                joiner.add( s );
            }
            String query = "ALTER TABLE " + request.table + " ADD PRIMARY KEY " + joiner.toString();
            try{
                int rows = handler.executeUpdate( query );
                handler.commit();
                result = new Result( new Debug().setAffectedRows( rows ).setGeneratedQuery( query ) );
            } catch ( SQLException | CatalogTransactionException e ){
                result = new Result( e.getMessage() );
            }
        }else{
            result = new Result( "Cannot add primary key if no columns are provided." );
        }
        return result;
    }


    /**
     * Get indexes of a table
     */
    Result getIndexes( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        String query = String.format( "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname ='%s' AND tablename = '%s'", request.schema, request.table );
        //todo use prepared statement
        LocalTransactionHandler handler = getHandler();
        Result result;
        ArrayList<String[]> data = new ArrayList<>();
        DbColumn[] header = { new DbColumn( "name" ), new DbColumn( "method" ), new DbColumn( "columns" ) };
        try ( ResultSet rs = handler.executeSelect( query ) ) {
            while ( rs.next() ) {
                String indexDef = rs.getString( "indexDef" );
                Pattern p = Pattern.compile( "\\((.*?)\\)" );
                Matcher m = p.matcher( indexDef );
                if ( !m.find() ) {
                    continue;
                }
                String colsRaw = m.group( 1 );
                String[] cols = colsRaw.split( ",\\s*" );
                data.add( new Index( request.schema, request.table, rs.getString( "indexname" ), "btree", cols ).asRow() );
            }
            result = new Result( header, data.toArray( new String[data.size()][3] ) );
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
        SidebarElement[] nodes = new SidebarElement[ pages.length ];
        int counter = 0;
        for( InformationPage page: pages ){
            nodes[counter] = new SidebarElement( page.getId(), page.getName(), analyzerId + "/", page.getIcon() );
            counter++;
        }
        try{
            CrudWebSocket.sendPageList( this.gson.toJson( nodes ) );
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
