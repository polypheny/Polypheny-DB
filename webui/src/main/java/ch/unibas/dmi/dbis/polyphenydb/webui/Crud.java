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


import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Debug;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Result;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SidebarElement;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SortState;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.UIRequest;
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
import java.util.StringJoiner;
import org.apache.commons.lang.math.NumberUtils;
import spark.Request;
import spark.Response;


/**
 * Create, read, update and delete elements from a database
 * contains only demo data so far
 */
class Crud {

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
            System.out.println( "Missing command-line arguments. Please provied the following information:\n"
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
            System.err.println( "Could not load driver class." );
            e.printStackTrace();
        }

        //Time zone: https://stackoverflow.com/questions/26515700/mysql-jdbc-driver-5-1-33-time-zone-issue
        final String URL = driver + host + ":" + port + "/" + dbName;//"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

        try {
            this.conn =  DriverManager.getConnection( URL, USER, PASS );
        } catch ( SQLException e ) {
            System.err.println( "Could not connect to the Database" );
            e.printStackTrace();
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
            e.printStackTrace();
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

        result.setCurrentPage( request.currentPage ).setTable( request.tableId );
        int tableSize = getTableSize( request.tableId );
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

        ArrayList<SidebarElement> result = new ArrayList<>();

        try {
            //get schemas
            String query = "SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";
            Statement stmt = conn.createStatement();
            ResultSet schemas = stmt.executeQuery( query );
            while ( schemas.next() ) {
                String schema = schemas.getString( "table_schema" );
                SidebarElement schemaTree = new SidebarElement( schema, schema, "cui-layers" );

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
                        tables.add( new SidebarElement( schema + "." + tableName, tableName, "fa fa-table" ) );
                    }
                    schemaTree.addChild( new SidebarElement( "tables", "tables", "fa fa-table" ).addChildren( tables ).setRouterLink( "" ) );
                    stmt2.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                }

                //get views
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
                        views.add( new SidebarElement( schema + "." + view, view, "icon-eye" ) );
                    }

                    stmt2.close();
                } catch ( SQLException e ) {
                    e.printStackTrace();
                }
                SidebarElement sidebarViews = new SidebarElement( "views", "views", "icon-eye" ).setRouterLink( "" );
                sidebarViews.addChildren( views );
                schemaTree.addChild( sidebarViews );

                result.add( schemaTree );
            }
        } catch ( SQLException e ) {
            e.printStackTrace();
        }

        /*
        SidebarElement db = new SidebarElement( "tables", "tables", "fa fa-table" ).setRouterLink( "[]" );;
        db.addChildren( result );

        SidebarElement[] out = { db, sidebarViews };
        */
        return this.gson.toJson( result );
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
                    value = "NULL";
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
            e.printStackTrace();
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
                joiner.add( entry.getKey() + " LIKE '" + entry.getValue() + "%'"  );
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
        Result result;

        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery( request.query );
            result = buildResult( rs, request );
        } catch ( SQLException e ) {
            try {
                Statement stmt = conn.createStatement();
                int numOfRows = stmt.executeUpdate( request.query );
                result = new Result( new Debug().setAffectedRows( numOfRows ) );
            } catch ( SQLException e2 ) {
                try {
                    conn.rollback();
                } catch ( SQLException e3 ) {
                    result = new Result( "Could not rollback failed transaction." );
                }
                result = new Result( e2.getMessage() );
            }
        }

        return result.toJson();
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
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
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
                if ( NumberUtils.isNumber( entry.getValue() ) ) {
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
        } catch ( SQLException e ) {
            result = new Result( e.getMessage() ).setInfo( new Debug().setGeneratedQuery( builder.toString() ) );
        }
        return result.toJson();
    }

}
