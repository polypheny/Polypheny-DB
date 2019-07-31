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


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.processing.DataContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.processing.QueryProviderImpl;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;


/**
 *
 */
public class PolyphenyDbStatementHandle {

    private final PolyphenyDbConnectionHandle connection;
    private final int statementId;
    private volatile transient Iterator<Object> openResultSet;
    private volatile transient PolyphenyDbSignature signature;
    @Getter
    private final AtomicBoolean cancelFlag = new AtomicBoolean();
    private final JavaTypeFactory typeFactory;


    public PolyphenyDbStatementHandle( final PolyphenyDbConnectionHandle connection, final int statementId, final JavaTypeFactory typeFactory ) {
        this.connection = connection;
        this.statementId = statementId;
        this.typeFactory = typeFactory;
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
    }


    public synchronized PolyphenyDbSignature getSignature() {
        return signature;
    }


    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }


    public void unset() {
        this.openResultSet = null;
        this.signature = null;
    }


    public DataContext getDataContext( final PolyphenyDbSchema rootSchema ) {
        Map<String, Object> map = new LinkedHashMap<>();
        // Avoid overflow
        int queryTimeout = RuntimeConfig.QUERY_TIMEOUT.getInteger();
        if ( queryTimeout > 0 && queryTimeout < Integer.MAX_VALUE / 1000 ) {
            map.put( DataContext.Variable.TIMEOUT.camelName, queryTimeout * 1000L );
        }

        final AtomicBoolean cancelFlag;
        cancelFlag = getCancelFlag();
        map.put( DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag );
        if ( RuntimeConfig.SPARK_ENGINE.getBoolean() ) {
            return new SlimDataContext();
        }
        return new DataContextImpl( new QueryProviderImpl(), map, rootSchema, typeFactory );
    }
}
