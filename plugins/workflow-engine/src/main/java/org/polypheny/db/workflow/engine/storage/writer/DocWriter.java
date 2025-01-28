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

import java.util.Iterator;
import java.util.List;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.DocMetadata;
import org.polypheny.db.workflow.engine.storage.DocBatchWriter;

public class DocWriter extends CheckpointWriter {

    /**
     * How many documents are inspected for metadata extraction.
     * A higher value results in a larger overhead.
     */
    private static final int MAX_INSPECTIONS = 100;

    private final DocBatchWriter writer;
    private long writeCount = 0;


    public DocWriter( LogicalCollection collection, Transaction transaction, DocMetadata metadata ) {
        super( collection, transaction, metadata );
        writer = new DocBatchWriter( collection, transaction );
    }


    public void write( PolyDocument document ) {
        if ( writeCount < MAX_INSPECTIONS ) {
            metadata.asDoc().addFields( document );
        }
        writeCount++;
        writer.write( document );
    }


    public void writeFromIterator( Iterator<PolyDocument> iterator ) {
        while ( iterator.hasNext() ) {
            write( iterator.next() );
        }
    }


    @Override
    public void write( List<PolyValue> tuple ) {
        write( tuple.get( 0 ).asDocument() );
    }


    @Override
    public long getWriteCount() {
        return writeCount;
    }


    @Override
    public void close() throws Exception {
        if ( transaction.isActive() ) { // ensure writer is only closed once
            try {
                writer.close();
            } finally {
                super.close();
            }
        }
    }


    private LogicalCollection getCollection() {
        return (LogicalCollection) entity;
    }

}
