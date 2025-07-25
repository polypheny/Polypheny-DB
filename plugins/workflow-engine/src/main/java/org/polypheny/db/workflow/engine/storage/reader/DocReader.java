/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.engine.storage.reader;

import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.DocMetadata;
import org.polypheny.db.workflow.engine.storage.QueryUtils;

@Slf4j
public class DocReader extends CheckpointReader {

    public static final int PREVIEW_DOCS_LIMIT = 100;


    public DocReader( LogicalCollection collection, Transaction transaction, DocMetadata metadata ) {
        super( collection, transaction, metadata );
    }


    public long getDocCount() {
        long count = metadata.getTupleCount();
        if ( count < 0 ) {
            LogicalCollection collection = getCollection();
            log.warn( "DocMetadata for {} is missing the tuple count. Performing count query as fallback.", collection.getName() );
            String query = "db.\"" + collection.getName() + "\".count({})";

            ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                    query, "MQL", collection.getNamespaceId(), transaction );

            Iterator<PolyValue[]> it = executedContext.getIterator().getIterator();
            registerIterator( it );
            try {
                PolyDocument doc = it.next()[0].asDocument();
                return doc.get( PolyString.of( "count" ) ).asNumber().longValue();
            } catch ( NoSuchElementException | IndexOutOfBoundsException | NullPointerException ignored ) {
                return 0;
            }
        }
        return count;
    }


    public Iterator<PolyDocument> getDocIterator() {
        Iterator<PolyValue[]> it = getArrayIterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }


            @Override
            public PolyDocument next() {
                return it.next()[0].asDocument();
            }
        };
    }


    public Iterable<PolyDocument> getDocIterable() {
        return this::getDocIterator;
    }


    @Override
    public AlgNode getAlgNode( AlgCluster cluster ) {
        return LogicalDocumentScan.create( cluster, entity );
    }


    @Override
    public Iterator<PolyValue[]> getArrayIterator() {
        LogicalCollection collection = getCollection();
        String query = "db.\"" + collection.getName() + "\".find({})";
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "MQL", collection.getNamespaceId(), transaction );
        Iterator<PolyValue[]> it = executedContext.getIterator().getIterator();
        registerIterator( it );
        return it;
    }


    @Override
    public long getTupleCount() {
        return getDocCount();
    }


    @Override
    public DataModel getDataModel() {
        return DataModel.DOCUMENT;
    }


    @Override
    public Triple<Result<?, ?>, Integer, Long> getPreview( @Nullable Integer maxTuples ) {
        int docsLimit = maxTuples == null ? PREVIEW_DOCS_LIMIT : Math.max( 0, maxTuples );
        LogicalCollection collection = getCollection();
        String query = "db.\"" + collection.getName() + "\".find({}).limit(" + docsLimit + ")";
        UIRequest request = UIRequest.builder()
                .entityId( collection.id )
                .namespace( collection.getNamespaceName() )
                .build();
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "MQL", collection.getNamespaceId(), transaction );

        return Triple.of( LanguageCrud.getDocResult( executedContext, request, executedContext.getStatement() ).build(),
                docsLimit,
                getDocCount() );
    }


    private LogicalCollection getCollection() {
        return (LogicalCollection) entity;
    }

}
