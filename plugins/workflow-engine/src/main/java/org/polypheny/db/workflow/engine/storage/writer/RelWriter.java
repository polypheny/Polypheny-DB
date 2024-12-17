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

package org.polypheny.db.workflow.engine.storage.writer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.workflow.engine.storage.BatchWriter;
import org.polypheny.db.workflow.engine.storage.QueryUtils;

public class RelWriter extends CheckpointWriter {

    private final boolean resetPk;
    private long currentPk = 0;
    private final int mapCapacity; // Since we know the size of each paramValue map, we can specify the initialCapacity for better performance

    private final BatchWriter writer;


    public RelWriter( LogicalTable table, Transaction transaction, boolean resetPk ) {
        super( table, transaction );
        this.resetPk = resetPk;
        this.mapCapacity = (int) Math.ceil( table.getTupleType().getFieldCount() / 0.75 ); // 0.75 is the default loadFactor of HashMap

        Map<Long, AlgDataType> paramTypes = new HashMap<>();
        StringJoiner joiner = new StringJoiner( ", ", "(", ")" );
        for ( int i = 0; i < table.getTupleType().getFieldCount(); i++ ) {
            joiner.add( "?" );
            AlgDataType fieldType = table.getTupleType().getFields().get( i ).getType();
            paramTypes.put( (long) i, fieldType );
        }

        String query = "INSERT INTO \"" + table.getName() + "\" VALUES " + joiner;
        QueryContext context = QueryUtils.constructContext( query, "SQL", table.getNamespaceId(), transaction );
        writer = new BatchWriter( context, transaction.createStatement(), paramTypes );
    }


    public void wAppend( List<PolyValue> row, PolyValue appendValue ) {
        Map<Long, PolyValue> map = getParamMap( row );
        map.put( (long) row.size(), appendValue );
        writeToBatch( map );
    }


    public void wInsert( List<PolyValue> row, PolyValue insertValue, int insertIdx ) {
        Map<Long, PolyValue> map = new HashMap<>( mapCapacity );

        for ( int i = 0; i < insertIdx; i++ ) {
            map.put( (long) i, row.get( i ) );
        }
        map.put( (long) insertIdx, insertValue );
        for ( int i = insertIdx + 1; i < row.size(); i++ ) {
            map.put( (long) i, row.get( i - 1 ) );
        }
        writeToBatch( map );
    }


    public void wReplace( List<PolyValue> row, PolyValue replaceValue, int replaceIdx ) {
        Map<Long, PolyValue> map = getParamMap( row );
        map.put( (long) replaceIdx, replaceValue );
        writeToBatch( map );
    }


    public void wRemove( List<PolyValue> row, int removeIdx ) {
        Map<Long, PolyValue> map = new HashMap<>( mapCapacity );

        for ( int i = 0; i < removeIdx; i++ ) {
            map.put( (long) i, row.get( i ) );
        }
        for ( int i = removeIdx + 1; i < row.size(); i++ ) {
            map.put( (long) i - 1, row.get( i ) );
        }
        writeToBatch( map );
    }


    /**
     * Writes a row with two columns: the primary key and the specified value.
     * Only use this method in combination with resetPk = true.
     *
     * @param value the value of the second column
     */
    public void write( PolyValue value ) {
        assert resetPk : "Writing a single value without generating a new primary key is never reasonable.";
        Map<Long, PolyValue> rowMap = new HashMap<>( mapCapacity );
        rowMap.put( 1L, value );
        writeToBatch( rowMap );
    }


    @Override
    public void write( List<PolyValue> row ) {
        writeToBatch( getParamMap( row ) );
    }


    @Override
    public void close() throws Exception {
        if ( transaction.isActive() ) { // ensure writer is only closed once
            writer.close();
            super.close();
        }
    }


    private void writeToBatch( Map<Long, PolyValue> rowMap ) {
        if ( resetPk ) {
            rowMap.put( 0L, PolyLong.of( currentPk ) );
            currentPk++;
        }
        writer.write( rowMap );
    }


    private Map<Long, PolyValue> getParamMap( List<PolyValue> row ) {
        Map<Long, PolyValue> map = new HashMap<>( mapCapacity );

        for ( int i = resetPk ? 1 : 0; i < row.size(); i++ ) {
            map.put( (long) i, row.get( i ) );
        }
        return map;
    }


    private LogicalTable getTable() {
        return (LogicalTable) entity;
    }


}
