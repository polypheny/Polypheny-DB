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

package ch.unibas.dmi.dbis.polyphenydb.jdbc.embedded;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbServerStatement;
import java.sql.SQLException;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.linq4j.Queryable;


/**
 * Implementation of {@link java.sql.Statement} for the Polypheny-DB engine.
 */
public abstract class PolyphenyDbEmbeddedStatement extends AvaticaStatement {

    /**
     * Creates a PolyphenyDbEmbeddedStatement.
     *
     * @param connection Connection
     * @param h Statement handle
     * @param resultSetType Result set type
     * @param resultSetConcurrency Result set concurrency
     * @param resultSetHoldability Result set holdability
     */
    PolyphenyDbEmbeddedStatement( PolyphenyDbEmbeddedConnectionImpl connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
        super( connection, h, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    @Override
    public <T> T unwrap( Class<T> iface ) throws SQLException {
        if ( iface == PolyphenyDbServerStatement.class ) {
            final PolyphenyDbServerStatement statement;
            try {
                statement = getConnection().server.getStatement( handle );
            } catch ( NoSuchStatementException e ) {
                throw new AssertionError( "invalid statement", e );
            }
            return iface.cast( statement );
        }
        return super.unwrap( iface );
    }


    @Override
    public PolyphenyDbEmbeddedConnectionImpl getConnection() {
        return (PolyphenyDbEmbeddedConnectionImpl) connection;
    }


    protected <T> PolyphenyDbSignature<T> prepare( Queryable<T> queryable ) {
        final PolyphenyDbEmbeddedConnectionImpl connection = getConnection();
        final PolyphenyDbPrepare prepare = connection.prepareFactory.apply();
        final PolyphenyDbServerStatement serverStatement;
        try {
            serverStatement = connection.server.getStatement( handle );
        } catch ( NoSuchStatementException e ) {
            throw new AssertionError( "invalid statement", e );
        }
        final Context prepareContext = serverStatement.createPrepareContext();
        return prepare.prepareQueryable( prepareContext, queryable );
    }


    @Override
    protected void close_() {
        if ( !closed ) {
            ((PolyphenyDbEmbeddedConnectionImpl) connection).server.removeStatement( handle );
            super.close_();
        }
    }
}
