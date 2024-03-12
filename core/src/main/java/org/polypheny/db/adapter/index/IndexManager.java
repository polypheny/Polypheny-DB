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


import com.google.common.collect.ImmutableList;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.DataStore.IndexMethodModel;
import org.polypheny.db.adapter.index.Index.IndexFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationKeyValue;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;


public class IndexManager {

    public static final String POLYPHENY = "POLYPHENY";
    private static final IndexManager INSTANCE = new IndexManager();

    private final AtomicLong indexLookupHitsCounter = new AtomicLong();
    private final AtomicLong indexLookupNoIndexCounter = new AtomicLong();
    private final AtomicLong indexLookupMissesCounter = new AtomicLong();

    private static final List<IndexFactory> INDEX_FACTORIES = Arrays.asList(
            new CoWHashIndex.Factory(),
            new CowMultiHashIndex.Factory()
    );

    private final Map<Long, Index> indexById = new HashMap<>();
    private final Map<String, Index> indexByName = new HashMap<>();
    private final Map<PolyXid, List<Index>> openTransactions = new HashMap<>();
    private TransactionManager transactionManager = null;


    public static IndexManager getInstance() {
        return INSTANCE;
    }


    private IndexManager() {
        registerMonitoringPage();
    }


    public static List<IndexMethodModel> getAvailableIndexMethods() {
        return ImmutableList.of(
                new IndexMethodModel( "hash", "HASH" )
        );
    }


    public static IndexMethodModel getDefaultIndexMethod() {
        return getAvailableIndexMethods().get( 0 );
    }


    void begin( PolyXid xid, Index index ) {
        if ( !openTransactions.containsKey( xid ) ) {
            openTransactions.put( xid, new ArrayList<>() );
        }
        openTransactions.get( xid ).add( index );
    }


    public void barrier( PolyXid xid ) {
        List<Index> idxs = openTransactions.get( xid );
        if ( idxs == null ) {
            return;
        }
        for ( final Index idx : idxs ) {
            idx.barrier( xid );
        }
    }


    public void commit( PolyXid xid ) {
        List<Index> idxs = openTransactions.remove( xid );
        if ( idxs == null ) {
            return;
        }
        for ( final Index idx : idxs ) {
            idx.barrier( xid );
        }
        for ( final Index idx : idxs ) {
            idx.commit( xid );
        }
    }


    public void rollback( PolyXid xid ) {
        List<Index> idxs = openTransactions.remove( xid );
        if ( idxs == null ) {
            return;
        }
        for ( final Index idx : idxs ) {
            idx.rollback( xid );
        }
    }


    public void initialize( final TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    public void restoreIndexes() throws TransactionException {
        for ( final LogicalIndex index : Catalog.getInstance().getSnapshot().rel().getIndexes() ) {
            if ( index.location < 0 ) {
                addIndex( index );
            }
        }
    }


    public void addIndex( final LogicalIndex index ) throws TransactionException {
        addIndex( index, null );
    }


    public void addIndex( final LogicalIndex index, final Statement statement ) throws TransactionException {
        addIndex( index.id, index.name, index.key, index.method, index.unique, null, statement );
    }


    protected void addIndex( final long id, final String name, final LogicalKey key, final String method, final Boolean unique, final Boolean persistent, final Statement statement ) throws TransactionException {
        final IndexFactory factory = INDEX_FACTORIES.stream()
                .filter( it -> it.canProvide( method, unique, persistent ) )
                .findFirst()
                .orElseThrow( IllegalArgumentException::new );
        final LogicalTable table = statement.getTransaction().getSnapshot().rel().getTable( key.entityId ).orElseThrow();
        final LogicalPrimaryKey pk = statement.getTransaction().getSnapshot().rel().getPrimaryKey( table.primaryKey ).orElseThrow();
        final Index index = factory.create(
                id,
                name,
                method,
                unique,
                persistent,
                Catalog.getInstance().getSnapshot().getNamespace( key.namespaceId ).orElseThrow(),
                table,
                key.getFieldNames(),
                pk.getFieldNames() );
        indexById.put( id, index );
        indexByName.put( name, index );
        final Transaction tx = statement.getTransaction();
        index.rebuild( tx );
    }


    public void deleteIndex( final LogicalIndex index ) {
        deleteIndex( index.id );
    }


    public void deleteIndex( final long indexId ) {
        final Index idx = indexById.remove( indexId );
        indexByName.remove( idx.name );
    }


    public Index getIndex( LogicalNamespace schema, LogicalTable table, List<String> columns ) {
        return this.indexById.values().stream().filter( index ->
                index.schema.equals( schema )
                        && index.table.equals( table )
                        && index.columns.equals( columns )
                        && index.isInitialized()
        ).findFirst().orElse( null );
    }


    public Index getIndex( LogicalNamespace schema, LogicalTable table, List<String> columns, String method, Boolean unique, Boolean persistent ) {
        return this.indexById.values().stream().filter( index ->
                index.schema.equals( schema )
                        && index.table.equals( table )
                        && index.columns.equals( columns )
                        && (method == null || (index.getMethod().equals( method )))
                        && (unique == null || (index.isUnique() == unique))
                        && (persistent == null || (index.isPersistent() == persistent))
        ).findFirst().orElse( null );
    }


    public List<Index> getIndices( LogicalNamespace schema, LogicalTable table ) {
        return this.indexById.values().stream()
                .filter( index -> index.schema.equals( schema ) && index.table.equals( table ) )
                .collect( Collectors.toList() );
    }


    public void incrementHit() {
        indexLookupHitsCounter.incrementAndGet();
    }


    public void incrementNoIndex() {
        indexLookupNoIndexCounter.incrementAndGet();
    }


    public void incrementMiss() {
        indexLookupMissesCounter.incrementAndGet();
    }


    public void resetCounters() {
        indexLookupHitsCounter.set( 0 );
        indexLookupNoIndexCounter.set( 0 );
        indexLookupMissesCounter.set( 0 );
    }


    private void registerMonitoringPage() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Polystore Indexes" );
        im.addPage( page );

        // General
        InformationGroup generalGroup = new InformationGroup( page, "General" ).setOrder( 1 );
        im.addGroup( generalGroup );

        InformationKeyValue generalKv = new InformationKeyValue( generalGroup );
        im.registerInformation( generalKv );
        generalGroup.setRefreshFunction( () -> {
            generalKv.putPair( "Status", RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ? "Active" : "Disabled" );
            generalKv.putPair( "Simplification", RuntimeConfig.POLYSTORE_INDEXES_SIMPLIFY.getBoolean() ? "Active" : "Disabled" );
            generalKv.putPair( "Number of Indexes", String.valueOf( indexById.keySet().size() ) );
            generalKv.putPair( "Total Index Entries", String.valueOf( indexById.values().stream().map( Index::size ).reduce( Integer::sum ).orElse( 0 ) ) );
        } );

        // Hit ratio
        InformationGroup hitRatioGroup = new InformationGroup( page, "Table Scan Replacements" ).setOrder( 2 );
        im.addGroup( hitRatioGroup );

        InformationGraph hitInfoGraph = new InformationGraph(
                hitRatioGroup,
                GraphType.DOUGHNUT,
                new String[]{ "Replaced", "Not Replaced", "No Index Available" }
        );
        hitInfoGraph.setOrder( 1 );
        im.registerInformation( hitInfoGraph );

        InformationTable hitInfoTable = new InformationTable(
                hitRatioGroup,
                Arrays.asList( "Attribute", "Percent", "Absolute" )
        );
        hitInfoTable.setOrder( 2 );
        im.registerInformation( hitInfoTable );

        hitRatioGroup.setRefreshFunction( () -> {
            long hits = indexLookupHitsCounter.longValue();
            long misses = indexLookupMissesCounter.longValue();
            long noIndex = indexLookupNoIndexCounter.longValue();
            double hitPercent = (double) hits / (hits + misses + noIndex);
            double missesPercent = (double) misses / (hits + misses + noIndex);
            double noIndexPercent = (double) noIndex / (hits + misses + noIndex);

            hitInfoGraph.updateGraph(
                    new String[]{ "Replaced", "Not Replaced", "No Index Available" },
                    new GraphData<>( "heap-data", new Long[]{ hits, misses, noIndex } )
            );

            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
            symbols.setDecimalSeparator( '.' );
            DecimalFormat df = new DecimalFormat( "#.0", symbols );
            hitInfoTable.reset();
            hitInfoTable.addRow( "Replaced", df.format( hitPercent * 100 ) + " %", hits );
            hitInfoTable.addRow( "Not  Replaced", df.format( missesPercent * 100 ) + " %", misses );
            hitInfoTable.addRow( "No Index Available", df.format( noIndexPercent * 100 ) + " %", noIndex );
        } );

        // Invalidate cache
        InformationGroup invalidateGroup = new InformationGroup( page, "Reset" ).setOrder( 3 );
        im.addGroup( invalidateGroup );

        InformationText invalidateText = new InformationText( invalidateGroup, "Reset the Polystore Index statistics." );
        invalidateText.setOrder( 1 );
        im.registerInformation( invalidateText );

        InformationAction invalidateAction = new InformationAction( invalidateGroup, "Reset", parameters -> {
            IndexManager.getInstance().resetCounters();
            return "Successfully reset the polystyore index statistics!";
        } );
        invalidateAction.setOrder( 2 );
        im.registerInformation( invalidateAction );
    }

}
