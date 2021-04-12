/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.jdbc;


import java.util.Iterator;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.transaction.Statement;

/**
 *
 */
public class PolyphenyDbStatementHandle {

    private final PolyphenyDbConnectionHandle connection;
    private final int statementId;
    private volatile transient Iterator<Object> openResultSet;
    private volatile transient PolyphenyDbSignature signature;
    @Getter
    @Setter
    private volatile transient String preparedQuery;
    @Getter
    @Setter
    private volatile transient int maxRowCount;

    @Setter
    @Getter
    private Statement statement;

    @Getter
    private final StopWatch executionStopWatch = new StopWatch();


    public PolyphenyDbStatementHandle( final PolyphenyDbConnectionHandle connection, final int statementId ) {
        this.connection = connection;
        this.statementId = statementId;
    }


    public int getStatementId() {
        return statementId;
    }


    public PolyphenyDbConnectionHandle getConnection() {
        return connection;
    }


    public synchronized void setOpenResultSet( Iterator<Object> result ) {
        if ( this.openResultSet != null ) {
            //  this.openResultSet.close();
        }
        this.openResultSet = result;
    }


    public synchronized Iterator<Object> getOpenResultSet() {
        return openResultSet;
    }


    public synchronized void setSignature( PolyphenyDbSignature signature ) {
        this.signature = signature;
        this.openResultSet = null;
        executionStopWatch.reset();
    }


    public synchronized PolyphenyDbSignature getSignature() {
        return signature;
    }


    public void unset() {
        this.openResultSet = null;
        this.signature = null;
        statement.unset();
    }
}
