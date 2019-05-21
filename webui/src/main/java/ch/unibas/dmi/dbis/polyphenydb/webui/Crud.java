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
    private String driver = "jdbc:mysql://";
    private String host;
    private int port;
    private String dbName;
    private final int PAGESIZE = 4;

    Crud( String[] args ) {

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
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch ( ClassNotFoundException e ) {
            System.err.println( "Could not load driver class." );
            e.printStackTrace();
        }

        //Time zone: https://stackoverflow.com/questions/26515700/mysql-jdbc-driver-5-1-33-time-zone-issue
        final String URL = driver + host + ":" + port + "/" + dbName + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

        try {
            this.conn =  DriverManager.getConnection( URL, USER, PASS );
        } catch ( SQLException e ) {
            System.err.println( "Could not connect to the Database" );
            e.printStackTrace();
        }
    }


    private Integer getTableSize ( final String tableName ) {
        Integer size = null;
        try {
            Statement stmt = conn.createStatement();
            String query = "SELECT count(1) FROM " + tableName;
            PreparedStatement ps = conn.prepareStatement( query );
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
        Gson gson = new Gson();
        UIRequest request = gson.fromJson( req.body(), UIRequest.class );

        ArrayList<String[]> data = new ArrayList<>();
        ArrayList<DbColumn> header = new ArrayList<>();

        try {
            StringBuilder query = new StringBuilder();
            String where = "";
            if( request.filter != null) where = filterTable( request.filter );
            String orderBy = "";
            if ( request.sortState != null ) orderBy = sortTable( request.sortState );
            query.append( "SELECT * FROM " ).append( request.tableId ).append( where ).append( orderBy ).append( " LIMIT " ).append( (request.currentPage -1) * PAGESIZE ).append( "," ).append( PAGESIZE );
            PreparedStatement ps = conn.prepareStatement( query.toString() );
            ResultSet rs = ps.executeQuery();
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
                //todo sortstate from request
            }
            while ( rs.next() ) {
                String[] row = new String[numOfCols];
                for( int i = 1; i <= numOfCols; i++) {
                    row[i-1] = rs.getString( i );
                }
                data.add( row );
            }
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
        //System.out.println(gson.toJson( header ));
        Result<String> result = new Result<String>( header.toArray( new DbColumn[0] ) , data.toArray( new String[ 0 ][] ));
        result.setCurrentPage( request.currentPage ).setTable( request.tableId );
        int tableSize = getTableSize( request.tableId );
        result.setHighestPage( (int) Math.ceil( (double) tableSize / PAGESIZE ) );
        return result.toJson();
    }


    /**
     * returns a Tree (in json format) with the Tables of a Database
     */
    String getSchemaTree( final Request req, final Response res ) {

        ArrayList<SidebarElement> result = new ArrayList<>();

        try {
            Statement stmt = conn.createStatement();
            String query = "SELECT TABLE_NAME AS _tables FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?";
            PreparedStatement ps = conn.prepareStatement( query );
            ps.setString( 1, this.dbName );
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                String tableName = rs.getString( "_tables" );
                //System.out.println( tableName);
                result.add( new SidebarElement( tableName, tableName, "fa fa-table" ) );
            }
            stmt.close();
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
        SidebarElement db = new SidebarElement( "db", "db", "fa fa-database" );
        db.setChildren( result );
        Gson gson = new Gson();
        return gson.toJson( db );
    }


    /**
     * insert data into a table
     */
    int insertIntoTable( final Request req, final Response res ) {
        int rowsAffected = 0;
        try {
            Gson gson = new Gson();
            UIRequest request = gson.fromJson( req.body() , UIRequest.class );

            Statement stmt = conn.createStatement();
            StringBuilder query = new StringBuilder().append( "INSERT INTO " ).append( request.tableId ).append( " VALUES " );
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
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
        return rowsAffected;
    }


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

}
