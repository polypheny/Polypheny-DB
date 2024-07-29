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

package org.polypheny.db.adapter.index;


import java.util.List;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


public class DeferredIndexUpdate {

    private enum Operation {
        INSERT,
        DELETE
    }


    private final Operation operation;
    private final Iterable<Pair<List<PolyValue>, List<PolyValue>>> insertTuples;
    private final Iterable<List<PolyValue>> deleteTuples;


    private DeferredIndexUpdate(
            final Operation operation,
            final Iterable<Pair<List<PolyValue>, List<PolyValue>>> insertTuples,
            final Iterable<List<PolyValue>> deleteTuples ) {
        this.operation = operation;
        this.insertTuples = insertTuples;
        this.deleteTuples = deleteTuples;
    }


    public void execute( final Index index ) {
        switch ( operation ) {
            case INSERT:
                index.insertAll( insertTuples );
                break;
            case DELETE:
                if ( insertTuples != null ) {
                    index.deleteAllPrimary( insertTuples );
                } else {
                    index.deleteAll( deleteTuples );
                }
                break;
        }
    }


    public static DeferredIndexUpdate createInsert( final Iterable<Pair<List<PolyValue>, List<PolyValue>>> tuples ) {
        return new DeferredIndexUpdate( Operation.INSERT, tuples, null );
    }


    public static DeferredIndexUpdate createDelete( final Iterable<List<PolyValue>> tuples ) {
        return new DeferredIndexUpdate( Operation.DELETE, null, tuples );
    }


    public static DeferredIndexUpdate createDeletePrimary( final Iterable<Pair<List<PolyValue>, List<PolyValue>>> tuples ) {
        return new DeferredIndexUpdate( Operation.DELETE, tuples, null );
    }

}
