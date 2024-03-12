/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.avatica;


import java.util.Iterator;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.transaction.Statement;

/**
 *
 */
@Getter
public class PolyStatementHandle<E> {

    private final PolyConnectionHandle connection;
    private final int statementId;
    private volatile transient Iterator<E> openResultSet;
    private volatile transient PolySignature signature;
    @Setter
    private volatile transient String preparedQuery;
    @Setter
    private volatile transient int maxRowCount;

    @Setter
    private Statement statement;

    @Getter
    private final StopWatch executionStopWatch = new StopWatch();


    public PolyStatementHandle( final PolyConnectionHandle connection, final int statementId ) {
        this.connection = connection;
        this.statementId = statementId;
    }


    public synchronized void setOpenResultSet( Iterator<E> result ) {
        this.openResultSet = result;
    }


    public synchronized void setSignature( PolySignature signature ) {
        this.signature = signature;
        this.openResultSet = null;
        executionStopWatch.reset();
    }


    public void unset() {
        this.openResultSet = null;
        this.signature = null;
        if ( statement != null ) {
            statement.close();
        }
    }

}
