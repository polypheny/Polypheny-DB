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


import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.ConstraintRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.DbColumn;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Debug;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.EditTableRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.Result;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.SchemaTreeRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.SidebarElement;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.ColumnRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.LocalTransactionHandler;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import spark.Request;
import spark.Response;


/**
 * Create, read, update and delete elements from a database
 * contains only demo data so far
 */
public class CrudPostgres extends Crud {


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
    CrudPostgres( final String driver, final String jdbc, final String host, final int port, final String dbName, final String user, final String pass ) {
        super( driver, jdbc, host, port, dbName, user, pass );
    }


    /**
     * returns a Tree (in json format) with the Tables of a Database
     */
    @Override
    ArrayList<SidebarElement> getSchemaTree ( final Request req, final Response res ) {
        SchemaTreeRequest request = this.gson.fromJson( req.body(), SchemaTreeRequest.class );
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
                        SidebarElement table = new SidebarElement( schema + "." + tableName, tableName, request.routerLinkRoot, "fa fa-table" );

                        if( request.depth > 2){
                            ResultSet columnsRs = handler.getMetaData().getColumns( this.dbName, schema, tableName, null );
                            while ( columnsRs.next() ){
                                String columnName = columnsRs.getString( 4 );
                                table.addChild( new SidebarElement( schema + "." + tableName + "." + columnName, columnName, request.routerLinkRoot ).setCssClass( "sidebarColumn" ) );
                            }
                        }
                        if( tablesRs.getString( 4 ).equals("TABLE") ){
                            tables.add( table );
                        } else if ( request.views && tablesRs.getString( 4 ).equals("VIEW") ){
                            views.add( table );
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
     * Create a new table
     */
    @Override
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
     * Filter a table with a keyword
     * Show only entries where the value of that column starts with the keyword
     *
     * @return the generated condition for the query
     */
    @Override
    protected String filterTable( final Map<String, String> filter ) {
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
     * Update a column of a table
     */
    @Override
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
    @Override
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
    @Override
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
     * Drop constraint of a table
     */
    @Override
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
    @Override
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

}
