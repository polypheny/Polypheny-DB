/*
 * Copyright 2019-2022 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.avatica;


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
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.util.Cursor;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.ArrayEnumeratorCursor;
import org.polypheny.db.runtime.ObjectEnumeratorCursor;


/**
 * Implementation of {@link ResultSet} for the Polypheny-DB engine.
 */
public class PolyphenyDbResultSet extends AvaticaResultSet {

    /**
     * Creates a PolyphenyDbResultSet.
     */
    public PolyphenyDbResultSet( AvaticaStatement statement, PolyphenyDbSignature polyphenyDbSignature, ResultSetMetaData resultSetMetaData, TimeZone timeZone, Meta.Frame firstFrame ) throws SQLException {
        super( statement, null, polyphenyDbSignature, resultSetMetaData, timeZone, firstFrame );
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
                        CursorFactory.deduce( columnMetaDataList, null ),
                        signature.rootSchema,
                        ImmutableList.of(),
                        -1,
                        null,
                        statement.getStatementType(),
                        new ExecutionTimeMonitor(),
                        signature.getSchemaType() );
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


    // Do not make public
    <T> PolyphenyDbSignature<T> getSignature() {
        //noinspection unchecked
        return (PolyphenyDbSignature) signature;
    }

}

