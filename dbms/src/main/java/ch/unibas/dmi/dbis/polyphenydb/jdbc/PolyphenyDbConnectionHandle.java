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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.PUID;
import ch.unibas.dmi.dbis.polyphenydb.PUID.ConnectionId;
import ch.unibas.dmi.dbis.polyphenydb.PUID.NodeId;
import ch.unibas.dmi.dbis.polyphenydb.PUID.UserId;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Utils;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import java.util.Objects;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.ConnectionProperties;


/**
 *
 */
public class PolyphenyDbConnectionHandle {

    private final Meta.ConnectionHandle handle;

    private final NodeId nodeId;
    private final UserId userId;

    private final CatalogUser user;
    private final CatalogDatabase database;
    private final CatalogSchema schema;

    private final ConnectionId connectionId;
    private volatile transient PolyXid currentTransaction;
    private volatile transient PolyphenyDbResultSet currentOpenResultSet;

    private volatile transient ConnectionProperties connectionProperties = new ConnectionPropertiesImpl( true, false, java.sql.Connection.TRANSACTION_SERIALIZABLE, "APP", "public" );


    public PolyphenyDbConnectionHandle( final Meta.ConnectionHandle handle, final NodeId nodeId, final CatalogUser catalogUser, final ConnectionId connectionId, final CatalogDatabase database, final CatalogSchema schema ) {
        this.handle = handle;

        this.nodeId = nodeId;
        this.userId = UserId.fromString( catalogUser.name ); // TODO: refactor CatalogUser
        this.user = catalogUser;
        this.connectionId = connectionId;
        this.database = database;
        this.schema = schema;
    }


    public PolyphenyDbConnectionHandle( final ConnectionHandle handle, final NodeId nodeId, final CatalogUser catalogUser, final String connectionId, final CatalogDatabase database, final CatalogSchema schema ) {
        this.handle = handle;

        this.nodeId = nodeId;
        this.userId = UserId.fromString( catalogUser.name ); // TODO: refactor CatalogUser
        this.user = catalogUser;
        this.connectionId = ConnectionId.fromString( connectionId );
        this.database = database;
        this.schema = schema;
    }


    public ConnectionId getConnectionId() {
        return connectionId;
    }


    public PolyXid startNewTransaction() {
        synchronized ( this ) {
            if ( currentTransaction != null ) {
                throw new IllegalStateException( "Illegal attempt to start a new transaction prior closing the current transaction." );
            }
            return currentTransaction = PolyphenyDbConnectionHandle.generateNewTransactionId( nodeId, userId, connectionId );
        }
    }


    //
    // DO NOT CALL THIS EXCEPT YOU KNOW WHAT YOU ARE DOING!!!!!!
    //
    // This method is only protected so that it can be called from the openConnection method in the DbmsMeta class. Do not call from elsewhere!
    protected static PolyXid generateNewTransactionId( final NodeId nodeId, final UserId userId, final ConnectionId connectionId ) {
        return Utils.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, PUID.randomPUID( PUID.Type.TRANSACTION ) );
    }


    public PolyXid getCurrentTransaction() {
        synchronized ( this ) {
            return currentTransaction;
        }
    }


    public PolyXid endCurrentTransaction() {
        synchronized ( this ) {
            PolyXid endedTransaction = currentTransaction;
            currentTransaction = null;
            return endedTransaction;
        }
    }


    public CatalogUser getUser() {
        return user;
    }


    public CatalogDatabase getDatabase() {
        return database;
    }


    public CatalogSchema getSchema() {
        return schema;
    }


    public void setCurrentOpenResultSet( PolyphenyDbResultSet resultSet ) {
        this.currentOpenResultSet = resultSet;
    }


    public ConnectionProperties mergeConnectionProperties( final ConnectionProperties connectionProperties ) {
        return this.connectionProperties.merge( connectionProperties );
    }


    public boolean isAutoCommit() {
        return this.connectionProperties.isAutoCommit();
    }


    public Meta.ConnectionHandle getHandle() {
        return this.handle;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        PolyphenyDbConnectionHandle that = (PolyphenyDbConnectionHandle) o;
        return Objects.equals( connectionId, that.connectionId );
    }


    @Override
    public int hashCode() {
        return Objects.hash( connectionId );
    }
}
