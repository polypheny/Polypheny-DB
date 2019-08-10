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
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.EditTableRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.requests.SchemaTreeRequest;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.LocalTransactionHandler;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringJoiner;
import spark.Request;
import spark.Response;


public class CrudPolypheny extends Crud {

    /**
     * Constructor
     *
     * @param driver driver name
     * @param jdbc jdbc url
     * @param host host name
     * @param port port
     * @param dbName database name
     * @param user user name
     * @param pass password
     */
    CrudPolypheny( final String driver, final String jdbc, final String host, final int port, final String dbName, final String user, final String pass ) {
        super( driver, jdbc, host, port, dbName, user, pass );
    }

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
                //if( schema.equals( "pg_catalog" ) || schema.equals( "information_schema" )) continue;
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
            /*if( col.defaultValue != null ) {
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
            }*/
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
            try {
                handler.rollback();
            } catch ( CatalogTransactionException ex ) {
                LOGGER.error( "Could not rollback CREATE TABLE statement: " + ex.getMessage(), ex );
            }
        }
        return result;
    }

    @Override
    protected String filterTable( final Map<String, String> filter ) {
        StringJoiner joiner = new StringJoiner( " AND ", " WHERE ", "" );
        int counter = 0;
        for ( Map.Entry<String, String> entry : filter.entrySet() ) {
            if ( ! entry.getValue().equals( "" )) {
                joiner.add( "CAST (" + entry.getKey() + " AS VARCHAR) LIKE '" + entry.getValue() + "%'"  );
                counter++;
            }
        }
        String out = "";
        if( counter > 0 ) out = joiner.toString();
        return out;
    }

    @Override
    Result updateColumn( final Request req, final Response res ) {
        return null;
    }

    @Override
    Result addColumn( final Request req, final Response res ) {
        return null;
    }

    @Override
    Result dropColumn( final Request req, final Response res ) {
        return null;
    }

    @Override
    Result dropConstraint( final Request req, final Response res ) {
        return null;
    }

    @Override
    Result addPrimaryKey( final Request req, final Response res ) {
        return null;
    }

}
