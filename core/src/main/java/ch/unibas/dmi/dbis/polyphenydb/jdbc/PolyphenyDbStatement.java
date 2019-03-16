/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.server.PolyphenyDbServerStatement;
import java.sql.SQLException;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.linq4j.Queryable;


/**
 * Implementation of {@link java.sql.Statement} for the Polypheny-DB engine.
 */
public abstract class PolyphenyDbStatement extends AvaticaStatement {

    /**
     * Creates a PolyphenyDbStatement.
     *
     * @param connection Connection
     * @param h Statement handle
     * @param resultSetType Result set type
     * @param resultSetConcurrency Result set concurrency
     * @param resultSetHoldability Result set holdability
     */
    PolyphenyDbStatement( PolyphenyDbConnectionImpl connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
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
    public PolyphenyDbConnectionImpl getConnection() {
        return (PolyphenyDbConnectionImpl) connection;
    }


    protected <T> PolyphenyDbSignature<T> prepare( Queryable<T> queryable ) {
        final PolyphenyDbConnectionImpl connection = getConnection();
        final PolyphenyDbPrepare prepare = connection.prepareFactory.apply();
        final PolyphenyDbServerStatement serverStatement;
        try {
            serverStatement = connection.server.getStatement( handle );
        } catch ( NoSuchStatementException e ) {
            throw new AssertionError( "invalid statement", e );
        }
        final PolyphenyDbPrepare.Context prepareContext = serverStatement.createPrepareContext();
        return prepare.prepareQueryable( prepareContext, queryable );
    }


    @Override
    protected void close_() {
        if ( !closed ) {
            ((PolyphenyDbConnectionImpl) connection).server.removeStatement( handle );
            super.close_();
        }
    }
}
