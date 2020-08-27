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

package org.polypheny.db.adapter;


import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.util.Pair;


public class DeferredIndexUpdate {

    private enum Operation {
        INSERT,
        DELETE,
        REVERSE_DELETE
    }


    private final Operation operation;
    @Getter
    private final long indexId;
    private final Set<Pair<List<RexLiteral>, List<RexLiteral>>> insertTuples;
    private final Set<List<RexLiteral>> deleteTuples;


    private DeferredIndexUpdate( final Operation operation, final long indexId, final Set<Pair<List<RexLiteral>, List<RexLiteral>>> insertTuples, final Set<List<RexLiteral>> deleteTuples ) {
        this.operation = operation;
        this.indexId = indexId;
        this.insertTuples = insertTuples;
        this.deleteTuples = deleteTuples;
    }


    public void execute() throws UnknownIndexException {
        final Index index = IndexManager.getInstance().getIndex( indexId );
        if (index == null) {
            throw new UnknownIndexException( indexId );
        }
        switch ( operation ) {
            case INSERT:
                index.insertAll( insertTuples );
                break;
            case DELETE:
                index.deleteAll( deleteTuples );
                break;
            case REVERSE_DELETE:
                index.reverseDeleteAll( deleteTuples );
                break;
        }
    }


    public static DeferredIndexUpdate createInsert( final long indexId, final Set<Pair<List<RexLiteral>, List<RexLiteral>>> tuples ) {
        return new DeferredIndexUpdate( Operation.INSERT, indexId, tuples, null );
    }


    public static DeferredIndexUpdate createDelete( final long indexId, final Set<List<RexLiteral>> tuples ) {
        return new DeferredIndexUpdate( Operation.DELETE, indexId, null, tuples );
    }


    public static DeferredIndexUpdate createReverseDelete( final long indexId, final Set<List<RexLiteral>> tuples ) {
        return new DeferredIndexUpdate( Operation.REVERSE_DELETE, indexId, null, tuples );
    }

}
