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


import ch.unibas.dmi.dbis.polyphenydb.PUID.ConnectionId;
import java.sql.ResultSet;
import java.util.Objects;
import javax.transaction.xa.Xid;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.ConnectionProperties;


/**
 *
 */
public class ConnectionImpl implements ConnectionHandle {

    private final Meta.ConnectionHandle handle;

    private final ConnectionId connectionId;
    private volatile transient Xid currentTransaction;
    private volatile transient ResultSet currentOpenResultSet;

    private volatile transient ConnectionProperties connectionProperties = new ConnectionPropertiesImpl( true, false, java.sql.Connection.TRANSACTION_SERIALIZABLE, "APP", "public" );


    public ConnectionImpl( final Meta.ConnectionHandle handle, final ConnectionId connectionId ) {
        this.handle = handle;
        this.connectionId = connectionId;
    }


    public ConnectionImpl( final Meta.ConnectionHandle handle, final String connectionId ) {
        this.handle = handle;
        this.connectionId = ConnectionId.fromString( connectionId );
    }


    @Override
    public ConnectionId getConnectionId() {
        return connectionId;
    }


    @Override
    public Xid startNewTransaction() {
        synchronized ( this ) {
            if ( currentTransaction != null ) {
                throw new IllegalStateException( "Illegal attempt to start a new transaction prior closing the current transaction." );
            }

            return currentTransaction = this.generateNewTransactionId( connectionId );
        }
    }


    @Override
    public Xid getCurrentTransaction() {
        synchronized ( this ) {
            return currentTransaction;
        }
    }


    @Override
    public Xid endCurrentTransaction() {
        synchronized ( this ) {
            Xid endedTransaction = currentTransaction;
            currentTransaction = null;
            return endedTransaction;
        }
    }


    @Override
    public void setCurrentOpenResultSet( ResultSet resultSet ) {
        synchronized ( this ) {
            this.currentOpenResultSet = resultSet;
        }
    }


    @Override
    public ConnectionProperties mergeConnectionProperties( final ConnectionProperties connectionProperties ) {
        synchronized ( this ) {
            return this.connectionProperties.merge( connectionProperties );
        }
    }


    @Override
    public boolean isAutoCommit() {
        synchronized ( this ) {
            return this.connectionProperties.isAutoCommit();
        }
    }


    @Override
    public Meta.ConnectionHandle getHandle() {
        return this.handle;
    }


    private Xid generateNewTransactionId( final ConnectionId connectionId ) {
        return null; // TODO
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        ConnectionImpl that = (ConnectionImpl) o;
        return Objects.equals( connectionId, that.connectionId );
    }


    @Override
    public int hashCode() {
        return Objects.hash( connectionId );
    }
}
