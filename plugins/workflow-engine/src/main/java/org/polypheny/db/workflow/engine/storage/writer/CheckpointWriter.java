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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;

public abstract class CheckpointWriter implements AutoCloseable {

    private static final long DEFAULT_BYTE_SIZE = 32; // used as fallback to estimate number of bytes in a PolyValue
    private static final long MAX_BYTES_PER_BATCH = 10 * 1024 * 1024L; // 10 MiB, upper limit to (estimated) size of batch in bytes
    private static final long MAX_TUPLES_PER_BATCH = 10_000; // upper limit to tuples per batch


    final LogicalEntity entity;
    final TransactionManager transactionManager;
    final Transaction transaction;


    public CheckpointWriter( LogicalEntity entity, Transaction transaction ) {
        this.entity = entity;
        this.transactionManager = transaction.getTransactionManager();
        this.transaction = transaction;
    }


    /**
     * Writes the given tuple to this checkpoint.
     * The tuple type of the checkpoint must be compatible with this tuple.
     * The list is guaranteed to remain unmodified.
     *
     * @param tuple the tuple to write to this checkpoint
     */
    public abstract void write( List<PolyValue> tuple );


    public final void write( Iterator<List<PolyValue>> iterator ) {
        while ( iterator.hasNext() ) {
            write( iterator.next() );
        }
    }


    @Override
    public void close() throws Exception {
        // even in case of an error we can commit, since the checkpoint will be dropped
        transaction.commit();
    }


    //////////////////
    // Static Utils //
    //////////////////
    static long computeBatchSize( PolyValue[] representativeTuple ) {
        long maxFromBytes = MAX_BYTES_PER_BATCH / estimateByteSize( representativeTuple );
        return Math.max( Math.min( maxFromBytes, MAX_TUPLES_PER_BATCH ), 1 );
    }


    private static long estimateByteSize( PolyValue[] tuple ) {
        long size = 0;
        for ( PolyValue value : tuple ) {
            try {
                size += value.getByteSize().orElse( getFallbackByteSize( value ) );
            } catch ( Exception e ) {
                size += DEFAULT_BYTE_SIZE;
            }
        }
        return size;
    }


    private static long estimateByteSize( Collection<? extends PolyValue> values ) {
        return estimateByteSize( values.toArray( new PolyValue[0] ) );
    }


    private static long estimateByteSize( PolyMap<? extends PolyValue, ? extends PolyValue> polyMap ) {
        return estimateByteSize( polyMap.getMap().keySet() ) +
                estimateByteSize( polyMap.getMap().values() );
    }


    private static long getFallbackByteSize( PolyValue value ) {

        return switch ( value.type ) {
            case DATE -> 16L;
            case SYMBOL -> 0L; // ?
            case ARRAY -> {
                if ( value instanceof PolyList<? extends PolyValue> polyList ) {
                    yield estimateByteSize( polyList.value );
                }
                yield DEFAULT_BYTE_SIZE;
            }
            case DOCUMENT, MAP -> {
                if ( value instanceof PolyMap<? extends PolyValue, ? extends PolyValue> polyMap ) {
                    yield estimateByteSize( polyMap );
                }
                yield DEFAULT_BYTE_SIZE;
            }
            case GRAPH -> {
                if ( value instanceof PolyGraph polyGraph ) {
                    yield estimateByteSize( polyGraph.getNodes() ) + estimateByteSize( polyGraph.getEdges() );
                }
                yield DEFAULT_BYTE_SIZE;
            }
            case EDGE, NODE -> {
                if ( value instanceof GraphPropertyHolder propHolder ) {
                    yield estimateByteSize( propHolder.properties ) + estimateByteSize( propHolder.labels );
                }
                yield DEFAULT_BYTE_SIZE;
            }
            case PATH -> {
                if ( value instanceof PolyPath polyPath ) {
                    yield estimateByteSize( polyPath.getNodes() ) +
                            estimateByteSize( polyPath.getEdges() ) +
                            estimateByteSize( polyPath.getPath() ) +
                            estimateByteSize( polyPath.getNames() ) +
                            estimateByteSize( polyPath.getSegments() );
                }
                yield DEFAULT_BYTE_SIZE;
            }
            case FILE -> {
                if ( value instanceof PolyBlob polyBlob ) {
                    yield polyBlob.value.length;
                }
                yield DEFAULT_BYTE_SIZE;
            }
            default -> DEFAULT_BYTE_SIZE;
        };
    }


}
