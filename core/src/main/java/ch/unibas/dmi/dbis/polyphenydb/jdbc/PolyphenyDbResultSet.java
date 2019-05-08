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
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayEnumeratorCursor;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ObjectEnumeratorCursor;
import com.google.common.collect.ImmutableList;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;


/**
 * Implementation of {@link ResultSet} for the Polypheny-DB engine.
 */
public class PolyphenyDbResultSet extends AvaticaResultSet {

    /**
     * Creates a PolyphenyDbResultSet.
     */
    PolyphenyDbResultSet( AvaticaStatement statement, PolyphenyDbSignature polyphenyDbSignature, ResultSetMetaData resultSetMetaData, TimeZone timeZone, Meta.Frame firstFrame ) throws SQLException {
        super( statement, null, polyphenyDbSignature, resultSetMetaData, timeZone, firstFrame );
    }


    @Override
    protected PolyphenyDbResultSet execute() throws SQLException {
        // Call driver's callback. It is permitted to throw a RuntimeException.
        PolyphenyDbEmbeddedConnectionImpl connection = getPolyphenyDbConnection();
        final boolean autoTemp = connection.config().autoTemp();
        Handler.ResultSink resultSink = null;
        if ( autoTemp ) {
            resultSink = () -> {
            };
        }
        connection.getDriver().handler.onStatementExecute( statement, resultSink );

        super.execute();
        return this;
    }


    @Override
    public ResultSet create( ColumnMetaData.AvaticaType elementType, Iterable<Object> iterable ) throws SQLException {
        final List<ColumnMetaData> columnMetaDataList;
        if ( elementType instanceof ColumnMetaData.StructType ) {
            columnMetaDataList = ((ColumnMetaData.StructType) elementType).columns;
        } else {
            columnMetaDataList = ImmutableList.of( ColumnMetaData.dummy( elementType, false ) );
        }
        final PolyphenyDbSignature signature = (PolyphenyDbSignature) this.signature;
        final PolyphenyDbSignature<Object> newSignature =
                new PolyphenyDbSignature<>(
                        signature.sql,
                        signature.parameters,
                        signature.internalParameters,
                        signature.rowType,
                        columnMetaDataList,
                        Meta.CursorFactory.ARRAY,
                        signature.rootSchema,
                        ImmutableList.of(),
                        -1,
                        null,
                        statement.getStatementType() );
        ResultSetMetaData subResultSetMetaData = new AvaticaResultSetMetaData( statement, null, newSignature );
        final PolyphenyDbResultSet resultSet = new PolyphenyDbResultSet( statement, signature, subResultSetMetaData, localCalendar.getTimeZone(), new Meta.Frame( 0, true, iterable ) );
        final Cursor cursor = resultSet.createCursor( elementType, iterable );
        return resultSet.execute2( cursor, columnMetaDataList );
    }


    private Cursor createCursor( ColumnMetaData.AvaticaType elementType, Iterable iterable ) {
        final Enumerator enumerator = Linq4j.iterableEnumerator( iterable );
        //noinspection unchecked
        return !(elementType instanceof ColumnMetaData.StructType) || ((ColumnMetaData.StructType) elementType).columns.size() == 1
                ? new ObjectEnumeratorCursor( enumerator )
                : new ArrayEnumeratorCursor( enumerator );
    }


    // do not make public
    <T> PolyphenyDbSignature<T> getSignature() {
        //noinspection unchecked
        return (PolyphenyDbSignature) signature;
    }


    // do not make public
    PolyphenyDbEmbeddedConnectionImpl getPolyphenyDbConnection() throws SQLException {
        return (PolyphenyDbEmbeddedConnectionImpl) statement.getConnection();
    }
}

