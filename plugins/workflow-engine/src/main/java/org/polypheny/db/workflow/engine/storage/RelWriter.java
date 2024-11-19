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

package org.polypheny.db.workflow.engine.storage;

import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.type.entity.PolyValue;

public class RelWriter extends CheckpointWriter {

    public RelWriter( LogicalTable table ) {
        super( table );
    }


    public void write( PolyValue[] row, PolyValue appendValue ) {
        PolyValue[] appended = new PolyValue[row.length + 1];
        System.arraycopy( row, 0, appended, 0, row.length );
        appended[row.length] = appendValue;
        write( appended );
    }


    public void wInserted( PolyValue[] row, PolyValue insertValue, int insertIdx ) {
        PolyValue[] inserted = new PolyValue[row.length + 1];
        System.arraycopy( row, 0, inserted, 0, insertIdx );
        inserted[insertIdx] = insertValue;
        System.arraycopy( row, insertIdx, inserted, insertIdx + 1, row.length - insertIdx );
        write( inserted );
    }


    public void wReplaced( PolyValue[] row, PolyValue replaceValue, int replaceIdx ) {
        PolyValue[] replaced = row.clone();
        replaced[replaceIdx] = replaceValue;
        write( replaced );
    }


    public void wReplacedInPlace( PolyValue[] row, PolyValue replaceValue, int replaceIdx ) {
        row[replaceIdx] = replaceValue;
        write( row );
    }


    public void wRemoved( PolyValue[] row, int removeIdx ) {
        PolyValue[] removed = new PolyValue[row.length - 1];
        System.arraycopy( row, 0, removed, 0, removeIdx );
        System.arraycopy( row, removeIdx + 1, removed, removeIdx, row.length - removeIdx - 1 );
        write( removed );
    }


    public void write( PolyValue value ) {
        write( new PolyValue[]{ value } );
    }


    @Override
    public void write( PolyValue[] row ) {
        throw new NotImplementedException();
    }

}
