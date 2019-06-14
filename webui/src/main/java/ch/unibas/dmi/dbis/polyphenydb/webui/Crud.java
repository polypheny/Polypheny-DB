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


import ch.unibas.dmi.dbis.polyphenydb.webui.models.ConstraintRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Debug;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.EditTableRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Index;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Result;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ResultType;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SidebarElement;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.UIRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.ColumnRequest;
import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.StringJoiner;
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
class Crud {

    private static final Logger LOGGER = LoggerFactory.getLogger( Crud.class );
    private Connection conn;
    //private String driver = "jdbc:mysql://";
    private String driver = "jdbc:postgresql://";
    private String host;
    private int port;
    private String dbName;
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
        this.host = args[0];
        this.port = Integer.parseInt( args[1] );
        this.dbName = args[2];
        final String USER = args[3];
        String pass = "";
        if ( args.length > 4 ) {
            pass = args[4];
        }
        final String PASS = pass;

        try {
            //Class.forName( "com.mysql.cj.jdbc.Driver" );
            Class.forName( "org.postgresql.Driver" );
        } catch ( ClassNotFoundException e ) {
            LOGGER.error( "Could not load driver class." );
            LOGGER.error( e.getMessage() );
        }

        //Time zone: https://stackoverflow.com/questions/26515700/mysql-jdbc-driver-5-1-33-time-zone-issue
        final String URL = driver + host + ":" + port + "/" + dbName;//"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

        try {
            this.conn =  DriverManager.getConnection( URL, USER, PASS );
        } catch ( SQLException e ) {
            LOGGER.error( "Could not connect to the Database" );
            LOGGER.error( e.getMessage() );
        }
    }


    /**
     * get the Number of rows in a table
     */
    private Integer getTableSize ( final String tableName ) {
        Integer size = null;
        try {
            Statement stmt = conn.createStatement();
            String query = "SELECT count(*) FROM " + tableName;
            PreparedStatement ps = conn.prepareStatement( query, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY );
            ResultSet rs = ps.executeQuery();
            rs.first();
            size = rs.getInt( 1 );
            stmt.close();
        } catch ( SQLException e ) {
            LOGGER.error( e.getMessage() );
        }
        return size;
    }


    /**
     * returns the content of a table
     * with a maximum of PAGESIZE elements
     */
    String getTable( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );

        ArrayList<String[]> data = new ArrayList<>();
        ArrayList<DbColumn> header = new ArrayList<>();
        Result result;

        try {
            StringBuilder query = new StringBuilder();
            String where = "";
            if( request.filter != null) where = filterTable( request.filter );
            String orderBy = "";
            if ( request.sortState != null ) orderBy = sortTable( request.sortState );
            query.append( "SELECT * FROM " ).append( request.tableId ).append( where ).append( orderBy ).append( " LIMIT " ).append( PAGESIZE ).append( " OFFSET " ).append( (request.currentPage - 1) * PAGESIZE );
            PreparedStatement ps = conn.prepareStatement( query.toString() );
            ResultSet rs = ps.executeQuery();
            result = buildResult( rs, request );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }

        //determine if it is a view or a table
        try {
            PreparedStatement ps = conn.prepareStatement( "SELECT table_type FROM information_schema.tables WHERE table_schema = ? AND table_name = ?" );
            String[] t = request.tableId.split( "\\." );
            ps.setString( 1, t[0] );
            ps.setString( 2, t[1] );
            ResultSet rs = ps.executeQuery();
            rs.next();//expecting only one result
            String type = rs.getString( "table_type" );
            switch ( type ){
                case "BASE TABLE":
                    result.setType( ResultType.TABLE );
                    break;
                default:
                    result.setType( ResultType.VIEW );
                }

        } catch ( SQLException e ) {
            LOGGER.error( e.toString() );
            result.setError( "Could not retrieve type of Result (table/view)." );
        }

        result.setCurrentPage( request.currentPage ).setTable( request.tableId );
        int tableSize = getTableSize( request.tableId );
        //todo if table is not existing: return result with error-message
        result.setHighestPage( (int) Math.ceil( (double) tableSize / PAGESIZE ) );
        return result.toJson();
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
                header.add( new DbColumn( meta.getColumnName( i ), sort, meta.getColumnType( i ), filter ) );
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
    String getSchemaTree( final Request req, final Response res ) {

        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        ArrayList<SidebarElement> result = new ArrayList<>();

        try {
            //get schemas
            String query = "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";
            Statement stmt = conn.createStatement();
            ResultSet schemas = stmt.executeQuery( query );
            while ( schemas.next() ) {
                String schema = schemas.getString( "table_schema" );
                SidebarElement schemaTree = new SidebarElement( schema, schema, "", "cui-layers" );

                try {
                    Statement stmt2 = conn.createStatement();
                    ArrayList<SidebarElement> tables = new ArrayList<>();
                    String query2 = "SELECT table_name AS _tables FROM information_schema.tables "
                            + "WHERE table_catalog = ? "
                            + "AND table_schema = ?"
                            + "AND table_type = 'BASE TABLE' "
                            + "AND table_schema NOT IN ('pg_catalog', 'information_schema')";
                    PreparedStatement ps = conn.prepareStatement( query2 );
                    ps.setString( 1, this.dbName );
                    ps.setString( 2, schema );
                    ResultSet rs = ps.executeQuery();
                    while ( rs.next() ) {
                        String tableName = rs.getString( "_tables" );
                        tables.add( new SidebarElement( schema + "." + tableName, tableName, request.routerLinkRoot, "fa fa-table" ) );
                    }
                    schemaTree.addChild( new SidebarElement( schema + ".tables", "tables", request.routerLinkRoot, "fa fa-table" ).addChildren( tables ).setRouterLink( "" ) );
                    stmt2.close();
                } catch ( SQLException e ) {
                    LOGGER.error( e.getMessage() );
                }

                if ( request.views ) {
                    //get views if requested
                    ArrayList<SidebarElement> views = new ArrayList<>();
                    try {
                        Statement stmt2 = conn.createStatement();
                        PreparedStatement ps = conn.prepareStatement( "SELECT table_name AS _tables FROM information_schema.tables "
                                + "WHERE table_catalog = ? "
                                + "AND table_schema = ? "
                                + "AND table_type = 'VIEW' "
                                + "AND table_schema NOT IN ('pg_catalog', 'information_schema')" );
                        ps.setString( 1, this.dbName );
                        ps.setString( 2, schema );
                        ResultSet rs = ps.executeQuery();
                        while ( rs.next() ) {
                            String view = rs.getString( 1 );
                            views.add( new SidebarElement( schema + "." + view, view, request.routerLinkRoot, "icon-eye" ) );
                        }

                        stmt2.close();
                    } catch ( SQLException e ) {
                        LOGGER.error( e.getMessage() );
                    }
                    SidebarElement sidebarViews = new SidebarElement( schema + ".views", "views", request.routerLinkRoot, "icon-eye" ).setRouterLink( "" );
                    sidebarViews.addChildren( views );
                    schemaTree.addChild( sidebarViews );
                }

                result.add( schemaTree );
            }
        } catch ( SQLException e ) {
            LOGGER.error( e.getMessage() );
        }

        return this.gson.toJson( result );
    }


    /**
     * Get all tables of a schema
     */
    String getTables( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Result result;
        try {
            String query2 = "SELECT table_name AS _tables FROM information_schema.tables "
                    + "WHERE table_catalog = ? "
                    + "AND table_schema = ?"
                    + "AND table_type = 'BASE TABLE' "
                    + "AND table_schema NOT IN ('pg_catalog', 'information_schema')";
            PreparedStatement ps = conn.prepareStatement( query2 );
            ps.setString( 1, this.dbName );
            ps.setString( 2, request.schema );
            ResultSet rs = ps.executeQuery();
            ArrayList<String> tables = new ArrayList<>();
            while ( rs.next() ) {
                tables.add( rs.getString( 1 ) );
            }
            result = new Result( new Debug().setAffectedRows( tables.size() ) ).setTables( tables );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }
        return result.toJson();
    }


    /**
     * Drop or truncate a table
     */
    String dropTruncateTable( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        Result result;
        StringBuilder query = new StringBuilder();
        if ( request.action.toLowerCase().equals( "drop" ) ) {
            query.append( "DROP TABLE " );
        } else if ( request.action.toLowerCase().equals( "truncate" ) ) {
            query.append( "TRUNCATE " );
        }
        query.append( request.schema ).append( "." ).append( request.table );
        try {
            Statement stmt = conn.createStatement();
            int a = stmt.executeUpdate( query.toString() );
            result = new Result( new Debug().setAffectedRows( 1 ).setGeneratedQuery( query.toString() ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
        }
        return result.toJson();
    }


    /**
     * Create a new table
     */
    String createTable( final Request req, final Response res ) {
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
            colBuilder.append( col.name ).append( " " ).append( col.type );
            if ( col.maxLength != null ) {
                colBuilder.append( String.format( "(%d)", col.maxLength ) );
            }
            if ( !col.nullable ) {
                colBuilder.append( " NOT NULL" );
            }
            if( col.defaultValue != null ) {
                switch ( col.type ) {
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
        try {
            Statement stmt = conn.createStatement();
            int a = stmt.executeUpdate( query.toString() );
            result = new Result( new Debug().setGeneratedQuery( query.toString() ).setAffectedRows( a ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
        }
        return result.toJson();
    }


    /**
     * insert data into a table
     */
    String insertIntoTable( final Request req, final Response res ) {
        int rowsAffected = 0;
        Result result;
        StringBuilder query = new StringBuilder();
        try {
            UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );

            Statement stmt = conn.createStatement();
            query.append( "INSERT INTO " ).append( request.tableId ).append( " VALUES " );
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( Map.Entry<String, String> entry : request.data.entrySet() ) {
                String value = entry.getValue();
                if( value.equals( "" ) ){
                    value = "DEFAULT";
                } else if( ! NumberUtils.isNumber( value )) {
                    value = "'"+value+"'";

                }
                joiner.add( value );
            }
            query.append( joiner.toString() );
            rowsAffected = stmt.executeUpdate( query.toString() );
            result = new Result( new Debug().setAffectedRows( rowsAffected ).setGeneratedQuery( query.toString() ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query.toString() ) );
            LOGGER.error( e.getMessage() );
            LOGGER.error( "Generated query: " + query.toString() );
        }
        return result.toJson();
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
    String anyQuery( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        ArrayList<Result> results = new ArrayList<>();
        //Result result;

        //todo set limit if it is not set or make it even smaller if is too large

        //Disable autoCommit if the query has commits.
        //ignore case: from: https://alvinalexander.com/blog/post/java/java-how-case-insensitive-search-string-matches-method
        Pattern p = Pattern.compile(".*(COMMIT|ROLLBACK).*", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher m = p.matcher(request.query);
        if( m.matches() ) {
            try {
                conn.setAutoCommit( false );
            } catch ( SQLException e ) {
                LOGGER.error( e.toString() );
            }
        }

        Pattern semicolon = Pattern.compile( ";[\\s]*$", Pattern.MULTILINE );//find all semicolons at the end of a line and split there
        String[] queries = semicolon.split( request.query );
        for ( String query: queries ) {
            Result result;
            Statement stmt = null;
            if( Pattern.matches( "(?si:[\\s]*COMMIT.*)", query ) ) {
                try {
                    conn.commit();
                    results.add( new Result( new Debug().setGeneratedQuery( query )) );
                } catch ( SQLException e ) {
                    LOGGER.error( e.toString() );
                }
            } else if( Pattern.matches( "(?si:[\\s]*ROLLBACK.*)", query ) ) {
                try {
                    conn.rollback();
                    results.add( new Result( new Debug().setGeneratedQuery( query )) );
                } catch ( SQLException e ) {
                    LOGGER.error( e.toString() );
                }
            } else if( Pattern.matches( "(?si:^[\\s]*SELECT.*)", query ) ) {
                try {
                    stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery( query );
                    result = buildResult( rs, request ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                } catch ( SQLException e ) {
                    result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                }
            } else {
                try {
                    stmt = conn.createStatement();
                    int numOfRows = stmt.executeUpdate( query );
                    result = new Result( new Debug().setAffectedRows( numOfRows ).setGeneratedQuery( query ) );
                    results.add( result );
                } catch ( SQLException e ) {
                    result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( query ) );
                    results.add( result );
                }
            }
        }

        //reset autoCommit to true
        try {
            conn.setAutoCommit( true );
        } catch ( SQLException e ) {
            LOGGER.error(e.toString());
        }

        return this.gson.toJson( results );
    }


    /**
     * delete a row from a table
     * the row is determined by the value of every column in that row (conjunction)
     * the transaction is being rolled back, if more that one row would be deleted
     */
    String deleteRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;
        StringBuilder builder = new StringBuilder();

        try {
            builder.append( "DELETE FROM " ).append( request.tableId ).append( " WHERE " );
            StringJoiner joiner = new StringJoiner( " AND ", "", "" );
            for ( Entry<String, String> entry : request.data.entrySet() ) {
                String condition = "";
                if ( entry.getValue() == null || entry.getValue().equals( "" ) ) {
                    //todo fix: doesn't work for integers
                    condition = String.format( "(%s IS NULL OR %s = '')", entry.getKey(), entry.getKey() );
                } else {
                    condition = String.format( "%s = '%s'", entry.getKey(), entry.getValue() );
                }
                joiner.add( condition );
            }
            builder.append( joiner.toString() );
            conn.setAutoCommit( false );
            Statement stmt = conn.createStatement();
            int numOfRows = stmt.executeUpdate( builder.toString() );
            //only commit if one row is deleted
            if ( numOfRows == 1 ) {
                conn.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                conn.rollback();
                result = new Result( "Attempt to delete " + numOfRows + " rows was blocked." ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
            conn.setAutoCommit( true );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            try {
                conn.rollback();
                conn.setAutoCommit( false );
            } catch ( SQLException e2 ) {
                result = new Result( e2.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        }
        return result.toJson();
    }


    String updateRow( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;
        StringBuilder builder = new StringBuilder();

        try {
            builder.append( "UPDATE " ).append( request.tableId ).append( " SET " );
            StringJoiner setStatements = new StringJoiner( ",", "", "" );
            for ( Entry<String, String> entry : request.data.entrySet() ) {
                if ( entry.getValue().equals( "" ) ) {
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

            conn.setAutoCommit( false );
            Statement stmt = conn.createStatement();
            int numOfRows = stmt.executeUpdate( builder.toString() );

            if ( numOfRows == 1 ) {
                conn.commit();
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } else {
                conn.rollback();
                result = new Result( "Attempt to update " + numOfRows + " rows was blocked." ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
            conn.setAutoCommit( true );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            try {
                conn.rollback();
                conn.setAutoCommit( true );
            } catch ( SQLException e2 ) {
                result = new Result( e2.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
            }
        }
        return result.toJson();
    }


    /**
     * Get the columns of a table
     */
    String getColumns( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );

        Result result;

        //query inspired from: https://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql

        try {
            PreparedStatement ps = conn.prepareStatement( "SELECT column_name, is_nullable, udt_name, character_maximum_length, column_default, "
                    + "constraint_type, constraint_name "
                    + "FROM( "
                    + "select DISTINCT ON (col.column_name) col.column_name, col.is_nullable, col.udt_name, col.character_maximum_length, col.column_default, "
                    + "tc.constraint_type, kc.constraint_name, col.ordinal_position "
                    + "FROM information_schema.columns col "
                    + "LEFT JOIN information_schema.key_column_usage AS kc "
                    + "ON col.column_name = kc.column_name "
                    + "LEFT JOIN information_schema.table_constraints tc "
                    + "ON tc.constraint_name = kc.constraint_name "
                    + "WHERE col.table_schema = ? "
                    + "AND col.table_name = ? "
                    + ") AS q1 "
                    + "ORDER BY ordinal_position ASC", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY );
            String[] t = request.tableId.split( "\\." );
            ps.setString( 1, t[0] );
            ps.setString( 2, t[1] );
            ResultSet rs = ps.executeQuery();
            ArrayList<DbColumn> cols = new ArrayList<>();
            while ( rs.next() ) {
                boolean isPrimary = false;
                if( rs.getString( "constraint_type" ) != null ){
                    isPrimary = rs.getString( "constraint_type" ).equals( "PRIMARY KEY" );
                }
                //getObject, so you get null and not 0 if the field is NULL
                cols.add( new DbColumn( rs.getString( "column_name" ), isPrimary, rs.getBoolean( "is_nullable" ), rs.getString( "udt_name" ), (Integer) rs.getObject("character_maximum_length"), rs.getString( "column_default" ) ) );
            }
            result = new Result( cols.toArray( new DbColumn[0] ), null );
        } catch ( SQLException e ) {
            result = new Result( e.toString() );
            LOGGER.error( e.toString() );
        }
        return result.toJson();
    }


    /**
     * Update a column of a table
     */
    String updateColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        DbColumn oldColumn = request.oldColumn;
        DbColumn newColumn = request.newColumn;
        Result result;
        StringBuilder generatedQueries = new StringBuilder();
        StringBuilder errors = new StringBuilder();

        //todo divide into more try catch blocks
        try {
            conn.setAutoCommit( false );

            //rename column if needed
            if ( !oldColumn.name.equals( newColumn.name ) ) {
                String query = String.format( "ALTER TABLE %s RENAME COLUMN %s TO %s", request.tableId, oldColumn.name, newColumn.name );
                PreparedStatement ps1 = conn.prepareStatement( query );
                generatedQueries.append( query );
                ps1.executeUpdate();
            }

            //change type + length
            if ( !oldColumn.type.equals( newColumn.type ) || !Objects.equals( oldColumn.maxLength, newColumn.maxLength ) ) {
                PreparedStatement ps2;
                if ( newColumn.maxLength != null ) {
                    String query = String.format( "ALTER TABLE %s ALTER COLUMN %s TYPE %s(%s) USING %s::%s;", request.tableId, newColumn.name, newColumn.type, newColumn.maxLength, newColumn.name, newColumn.type );
                    generatedQueries.append( query );
                    ps2 = conn.prepareStatement( query );
                } else {
                    //todo drop maxlength if requested
                    String query = String.format( "ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s;", request.tableId, newColumn.name, newColumn.type, newColumn.name, newColumn.type );
                    ps2 = conn.prepareStatement( query );
                    generatedQueries.append( query );
                }
                ps2.executeUpdate();
            }

            //set/drop nullable
            if ( oldColumn.nullable != newColumn.nullable ) {
                String nullable = "SET";
                if ( newColumn.nullable ) {
                    nullable = "DROP";
                }
                String query = "ALTER TABLE " + request.tableId + " ALTER COLUMN " + newColumn.name + " " + nullable + " NOT NULL";
                PreparedStatement ps3 = conn.prepareStatement( query );
                generatedQueries.append( query );
                ps3.executeUpdate();
            }

            //change default value
            if ( oldColumn.defaultValue == null || newColumn.defaultValue == null || !oldColumn.defaultValue.equals( newColumn.defaultValue ) ){
                String query;
                if( newColumn.defaultValue == null ){
                    query = String.format( "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", request.tableId, newColumn.name );
                }
                else{
                    query = String.format( "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT ", request.tableId, newColumn.name );
                    switch ( newColumn.type ) {
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
                PreparedStatement ps4 = conn.prepareStatement( query );
                generatedQueries.append( query );
                ps4.executeUpdate();
            }

            result = new Result( new Debug().setAffectedRows( 1 ).setGeneratedQuery( generatedQueries.toString() ) );
            conn.commit();
            conn.setAutoCommit( true );
            System.out.println(generatedQueries);
        } catch ( SQLException e ) {
            result = new Result( e.toString() ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( generatedQueries.toString() ) );
            try {
                conn.rollback();
                conn.setAutoCommit( true );
            } catch ( SQLException e2 ) {
                result = new Result( e2.toString() ).setInfo( new Debug().setAffectedRows( 0 ).setGeneratedQuery( generatedQueries.toString() ) );
            }
        }

        return result.toJson();
    }


    /**
     * Add a column to an existing table
     */
    String addColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        String query = String.format( "ALTER TABLE %s ADD COLUMN %s %s", request.tableId, request.newColumn.name, request.newColumn.type );
        if ( request.newColumn.maxLength != null ) {
            query = query + String.format( "(%d)", request.newColumn.maxLength );
        }
        if ( !request.newColumn.nullable ) {
            query = query + " NOT NULL";
        }
        if ( request.newColumn.defaultValue != null ){
            switch ( request.newColumn.type ) {
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
            Statement stmt = conn.createStatement();
            System.out.println(query);
            int affectedRows = stmt.executeUpdate( query );
            result = new Result( new Debug().setAffectedRows( affectedRows ).setGeneratedQuery( query ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }

        return result.toJson();
    }


    /**
     * Delete a column of a table
     */
    String dropColumn( final Request req, final Response res ) {
        ColumnRequest request = this.gson.fromJson( req.body(), ColumnRequest.class );
        Result result;
        String query = String.format( "ALTER TABLE %s DROP COLUMN %s", request.tableId, request.oldColumn.name );
        try {
            PreparedStatement ps = conn.prepareStatement( query );
            int affectedRows = ps.executeUpdate();
            result = new Result( new Debug().setAffectedRows( affectedRows ) );
        } catch ( SQLException e ) {
            result = new Result( e.toString() );
        }
        return result.toJson();
    }


    /**
     * Get constraints of a table
     */
    String getConstraints( final Request req, final Response res ) {
        UIRequest request = this.gson.fromJson( req.body(), UIRequest.class );
        Result result;
        String query = "SELECT constraint_name, constraint_type, is_deferrable, initially_deferred "
                + "FROM information_schema.table_constraints tc "
                + "WHERE table_schema = ? AND table_name = ? "
                + "AND constraint_name NOT LIKE '%not_null'";
        try{
            PreparedStatement ps = conn.prepareStatement( query );
            String[] t = request.tableId.split( "\\." );
            ps.setString( 1, t[0] );
            ps.setString( 2, t[1] );
            ResultSet rs = ps.executeQuery();
            result = buildResult( rs, request );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }
        return result.toJson();
    }


    /**
     * Drop consraint of a table
     */
    String dropConstraint ( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
        String query = String.format( "ALTER TABLE %s DROP CONSTRAINT %s;", request.table, request.constraint.name );
        Result result;
        try{
            Statement stmt = conn.createStatement();
            int rows = stmt.executeUpdate( query );
            result = new Result( new Debug().setAffectedRows( rows ) );
        } catch ( SQLException e ){
            result = new Result( e.getMessage() );
        }
        return result.toJson();
    }

    /**
     * Add a primary key to a table
     */
    String addPrimaryKey ( final Request req, final Response res ) {
        ConstraintRequest request = this.gson.fromJson( req.body(), ConstraintRequest.class );
        Result result;
        if( request.constraint.columns.length > 0 ){
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for( String s : request.constraint.columns ){
                joiner.add( s );
            }
            String query = "ALTER TABLE " + request.table + " ADD PRIMARY KEY " + joiner.toString();
            try{
                Statement stmt = conn.createStatement();
                int rows = stmt.executeUpdate( query );
                result = new Result( new Debug().setAffectedRows( rows ).setGeneratedQuery( query ) );
            } catch ( SQLException e ){
                result = new Result( e.getMessage() );
            }
        }else{
            result = new Result( "Cannot add primary key if no columns are provided." );
        }
        return result.toJson();
    }


    /**
     * Get indexes of a table
     */
    String getIndexes( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        String query = "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = ? AND tablename = ?";
        Result result;
        ArrayList<String[]> data = new ArrayList<>();
        DbColumn[] header = { new DbColumn( "name" ), new DbColumn( "method" ), new DbColumn( "columns" ) };
        try {
            PreparedStatement ps = conn.prepareStatement( query );
            ps.setString( 1, request.schema );
            ps.setString( 2, request.table );
            ResultSet rs = ps.executeQuery();
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
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }
        return result.toJson();
    }


    /**
     * Drop an index of a table
     */
    String dropIndex( final Request req, final Response res ) {
        EditTableRequest request = this.gson.fromJson( req.body(), EditTableRequest.class );
        String query = String.format( "DROP INDEX %s.%s", request.schema, request.action );
        Result result;
        try {
            Statement stmt = this.conn.createStatement();
            int a = stmt.executeUpdate( query );
            result = new Result( new Debug().setGeneratedQuery( query ).setAffectedRows( a ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }
        return result.toJson();
    }


    /**
     * Create an index for a table
     */
    String createIndex( final Request req, final Response res ) {
        Index index = this.gson.fromJson( req.body(), Index.class );
        Result result;
        try {
            Statement stmt = conn.createStatement();
            int a = stmt.executeUpdate( index.create() );
            result = new Result( new Debug().setAffectedRows( a ) );
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() );
        }
        return result.toJson();
    }

}
