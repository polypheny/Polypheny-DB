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

package org.polypheny.db.workflow.engine.storage.reader;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.engine.storage.QueryUtils;

public class RelReader extends CheckpointReader {


    public RelReader( LogicalTable table, Transaction transaction ) {
        super( table, transaction );
    }


    public long getRowCount() {
        LogicalTable table = getTable();
        String query = "SELECT COUNT(*) FROM \"" + table.getName() + "\"";
        Iterator<PolyValue[]> it = executeSqlQuery( query );
        try {
            return it.next()[0].asLong().longValue();
        } catch ( NoSuchElementException | IndexOutOfBoundsException | NullPointerException ignored ) {
            return 0;
        }
    }


    @Override
    public AlgNode getAlgNode( AlgCluster cluster ) {
        AlgTraitSet traits = AlgTraitSet.createEmpty().plus( ModelTrait.RELATIONAL );
        return new LogicalRelScan( cluster, traits, entity );
    }


    @Override
    public Iterator<PolyValue[]> getArrayIterator() {
        LogicalTable table = getTable();
        /* Alternatively: Batch reader approach
        Iterator<PolyValue[]> iterator = new AsyncRelBatchReader( table, transaction, StorageManager.PK_COL, true );
        registerIterator( iterator );
        return iterator;*/

        String query = "SELECT " + QueryUtils.quoteAndJoin( table.getColumnNames() ) + " FROM " + QueryUtils.quotedIdentifier( table );
        return executeSqlQuery( query );
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


    private String getQuotedColumns() {
        LogicalTable table = getTable();
        return table.getColumnNames().stream()
                .map( s -> "\"" + s + "\"" )
                .collect( Collectors.joining( ", " ) );

    }

}
