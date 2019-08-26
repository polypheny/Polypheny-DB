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

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.JdbcConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.TransactionHandler;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Model for a schema of a DBMS
 */
public class Schema {

    private static final Logger LOGGER = LoggerFactory.getLogger( Schema.class );

    private String name;
    private String type;//todo enum

    // fields for creation
    boolean create;
    boolean ifNotExists;
    String authorization;

    //fields for deletion
    boolean drop;
    boolean ifExists;
    boolean cascade;


    /**
     * Schema Constructor
     *
     * @param name name of the schema
     * @param type type of the schema, e.g. relational
     */
    public Schema( final String name, final String type ) {
        this.name = name;
        this.type = type;
    }


    /**
     * Create or drop a schema. If schema.create is true, it will be created, if schema.drop is true, it will be dropped.
     */
    public Result executeCreateOrDrop( final TransactionHandler handler ) {
        if ( this.create && !this.drop ) {
            return createSchema( handler );
        } else if ( this.drop ) {
            return dropSchema( handler );
        } else {
            return new Result( "Neither the field 'create' nor the field 'drop' was set." );
        }
    }


    /**
     * Create the query for the schema creation and execute it
     */
    private Result createSchema( final TransactionHandler handler ) {
        if ( !this.create && this.drop ) {
            return new Result( "Did not create schema " + this.name + " since the boolean 'create' was not set." );
        }
        StringBuilder query = new StringBuilder( "CREATE SCHEMA " );
        if ( this.ifExists ) {
            query.append( "IF NOT EXISTS " );
        }
        query.append( "\"" ).append( this.name ).append( "\"" );
        if ( this.authorization != null && !this.authorization.equals( "" ) ) {
            query.append( " AUTHORIZATION " ).append( this.authorization );
        }
        try {
            int rows = handler.executeUpdate( query.toString() );
            handler.commit();
            return new Result( new Debug().setAffectedRows( rows ) );
        } catch ( SQLException | JdbcConnectionException e ) {
            return new Result( e.getMessage() );
        }
    }


    /**
     * Create the query to drop this schema and execute it
     */
    private Result dropSchema( final TransactionHandler handler ) {
        if ( !this.drop ) {
            return new Result( "Did not drop schema " + this.name + " since the boolean 'drop' was not set." );
        }
        StringBuilder query = new StringBuilder( "DROP SCHEMA " );
        if ( this.ifExists ) {
            query.append( "IF EXISTS " );
        }
        query.append( "\"" ).append( this.name ).append( "\"" );
        if ( this.cascade ) {
            query.append( " CASCADE" );
        }
        try {
            int rows = handler.executeUpdate( query.toString() );
            handler.commit();
            return new Result( new Debug().setAffectedRows( rows ) );
        } catch ( SQLException | JdbcConnectionException e ) {
            return new Result( e.getMessage() );
        }
    }

}
