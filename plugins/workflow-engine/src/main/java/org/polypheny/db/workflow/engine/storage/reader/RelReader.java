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
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.RelMetadata;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;

@Slf4j
public class RelReader extends CheckpointReader {

    public static final int PREVIEW_ROW_LIMIT = 100;
    private final String quotedIdentifier;
    private final String quotedCols;


    public RelReader( LogicalTable table, Transaction transaction, RelMetadata metadata ) {
        super( table, transaction, metadata );
        this.quotedIdentifier = QueryUtils.quotedIdentifier( table );
        this.quotedCols = QueryUtils.quoteAndJoin( table.getColumnNames() );
    }


    public long getRowCount() {
        long count = metadata.getTupleCount();
        if ( count < 0 ) {
            log.warn( "RelMetadata for {} is missing the tuple count. Performing count query as fallback.", entity.getName() );
            String query = "SELECT COUNT(*) FROM " + quotedIdentifier;
            Iterator<PolyValue[]> it = executeSqlQuery( query );
            try {
                return it.next()[0].asNumber().longValue();
            } catch ( NoSuchElementException | IndexOutOfBoundsException | NullPointerException ignored ) {
                return 0;
            }
        }
        return count;
    }


    @Override
    public AlgNode getAlgNode( AlgCluster cluster ) {
        AlgTraitSet traits = cluster.traitSetOf( ModelTrait.RELATIONAL );
        return new LogicalRelScan( cluster, traits, entity );
    }


    @Override
    public Iterator<PolyValue[]> getArrayIterator() {
        String query = "SELECT " + quotedCols + " FROM " + quotedIdentifier;
        return executeSqlQuery( query );
    }


    @Override
    public long getTupleCount() {
        return getRowCount();
    }


    @Override
    public DataModel getDataModel() {
        return DataModel.RELATIONAL;
    }


    @Override
    public Triple<Result<?, ?>, Integer, Long> getPreview( @Nullable Integer maxTuples ) {
        int rowLimit = maxTuples == null ? PREVIEW_ROW_LIMIT : Math.max( 0, maxTuples );
        LogicalTable table = getTable();
        boolean isEmpty = table.getColumnNames().size() == 1;
        String quotedNoKey = QueryUtils.quoteAndJoin( table.getColumnNames().stream()
                .filter( n -> isEmpty || !n.equals( StorageManager.PK_COL ) ).toList() );
        String query = "SELECT " + quotedNoKey + " FROM " + quotedIdentifier
                + " LIMIT " + rowLimit;
        UIRequest request = UIRequest.builder()
                .entityId( table.id )
                .namespace( table.getNamespaceName() )
                .build();
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "SQL", table.getNamespaceId(), transaction );

        return Triple.of(
                QueryUtils.getRelResult( executedContext, request, executedContext.getStatement() ),
                rowLimit,
                getRowCount() );
    }


    private LogicalTable getTable() {
        return (LogicalTable) entity;
    }


    private Iterator<PolyValue[]> executeSqlQuery( String query ) {
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "SQL", getTable().getNamespaceId(), transaction );
        Iterator<PolyValue[]> iterator = executedContext.getIterator().getIterator();
        registerIterator( iterator );
        return iterator;
    }

}
