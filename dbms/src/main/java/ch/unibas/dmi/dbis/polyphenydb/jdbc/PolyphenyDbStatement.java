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


/**
 *
 */
public class PolyphenyDbStatement implements PolyphenyDbStatementHandle {

    private final PolyphenyDbConnectionHandle connection;
    private final int statementId;
    private volatile transient PolyphenyDbResultSet openResultSet;


    public PolyphenyDbStatement( final PolyphenyDbConnectionHandle connection, final int statementId ) {
        this.connection = connection;
        this.statementId = statementId;
    }


    @Override
    public int getStatementId() {
        return statementId;
    }


    @Override
    public PolyphenyDbConnectionHandle getConnection() {
        return connection;
    }


    @Override
    public synchronized void setOpenResultSet( PolyphenyDbResultSet result ) {
        if ( this.openResultSet != null ) {
            this.openResultSet.close();
        }
        this.openResultSet = result;

    }


    @Override
    public synchronized PolyphenyDbResultSet getOpenResultSet() {
        return openResultSet;
    }
}
