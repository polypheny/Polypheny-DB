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

package org.polypheny.db.workflow.engine.storage;

import static org.polypheny.db.workflow.engine.storage.LpgBatchWriter.BATCHABLE_LPG_ADAPTERS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.DocMetadata;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.LpgMetadata;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.RelMetadata;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;

@Slf4j
public class StorageManagerImpl implements StorageManager {

    public static final String REL_PREFIX = "rel_";
    public static final String DOC_PREFIX = "doc_";
    public static final String LPG_PREFIX = "lpg_";
    public static final String TABLE_PREFIX = "t_";
    public static final String COLLECTION_PREFIX = "c_";
    public static final AtomicBoolean STARTED_ADAPTER = new AtomicBoolean( false );

    private final UUID sessionId;
    private final Map<DataModel, String> defaultStores;
    private final Map<UUID, Map<Integer, Pair<LogicalEntity, CheckpointMetadata>>> checkpoints = new ConcurrentHashMap<>();
    private final Set<Pair<UUID, Integer>> linkedCheckpoints = ConcurrentHashMap.newKeySet();  // Checkpoints that are managed by a different StorageManager, but are linked to the output of an activity in this workflow
    private final Map<Long, String> registeredNamespaces = new ConcurrentHashMap<>();
    private final AdapterManager adapterManager;
    private final DdlManager ddlManager;
    private final TransactionManager transactionManager;

    private final Map<UUID, Transaction> localTransactions = new ConcurrentHashMap<>();
    private Transaction extractTransaction;
    private Transaction loadTransaction;

    private final long relNamespace;
    private final long docNamespace;


    public StorageManagerImpl( TransactionManager transactionManager, UUID sessionId, Map<DataModel, String> defaultStores ) {
        adapterManager = AdapterManager.getInstance();
        ddlManager = DdlManager.getInstance();
        this.transactionManager = transactionManager;

        this.sessionId = sessionId;
        this.defaultStores = new ConcurrentHashMap<>( defaultStores );
        String fallbackStore = DEFAULT_CHECKPOINT_ADAPTER;
        this.defaultStores.putIfAbsent( DataModel.RELATIONAL, fallbackStore );
        this.defaultStores.putIfAbsent( DataModel.DOCUMENT, fallbackStore );
        this.defaultStores.putIfAbsent( DataModel.GRAPH, fallbackStore );

        if ( !STARTED_ADAPTER.get() ) {
            addDefaultCheckpointAdapterIfMissing();
        }

        String relNsName = getNamespaceName( REL_PREFIX );
        relNamespace = ddlManager.createNamespace(
                relNsName, DataModel.RELATIONAL, true, false, true, null );
        registeredNamespaces.put( relNamespace, relNsName );

        String docNsName = getNamespaceName( DOC_PREFIX );
        docNamespace = ddlManager.createNamespace( docNsName, DataModel.DOCUMENT, true, false, true, null );
        registeredNamespaces.put( docNamespace, docNsName );
    }


    @Override
    public UUID getSessionId() {
        return sessionId;
    }


    @Override
    public String getDefaultStore( DataModel model ) {
        return defaultStores.get( model );
    }


    @Override
    public void setDefaultStore( DataModel model, String storeName ) {
        defaultStores.put( model, storeName );
    }


    @Override
    public CheckpointReader readCheckpoint( UUID activityId, int outputIdx ) {
        Pair<LogicalEntity, CheckpointMetadata> checkpoint = Objects.requireNonNull( checkpoints.get( activityId ).get( outputIdx ), "Checkpoint does not exist for output " + outputIdx + " of activity " + activityId );
        LogicalEntity entity = checkpoint.left;
        return switch ( entity.dataModel ) {
            case RELATIONAL -> new RelReader( (LogicalTable) entity, QueryUtils.startTransaction( transactionManager, entity.namespaceId, "RelRead" ), checkpoint.right.asRel() );
            case DOCUMENT -> new DocReader( (LogicalCollection) entity, QueryUtils.startTransaction( transactionManager, entity.namespaceId, "DocRead" ), checkpoint.right.asDoc() );
            case GRAPH -> new LpgReader( (LogicalGraph) entity, QueryUtils.startTransaction( transactionManager, entity.namespaceId, "LpgRead" ), checkpoint.right.asLpg() );
        };
    }


    @Override
    public DataModel getDataModel( UUID activityId, int outputIdx ) {
        return getCheckpoint( activityId, outputIdx ).dataModel;
    }


    @Override
    public AlgDataType getTupleType( UUID activityId, int outputIdx ) {
        return getCheckpoint( activityId, outputIdx ).getTupleType();
    }


    @Override
    public List<TypePreview> getCheckpointPreviewTypes( UUID activityId ) {
        List<TypePreview> previews = new ArrayList<>();
        Map<Integer, Pair<LogicalEntity, CheckpointMetadata>> outputs = checkpoints.get( activityId );
        if ( outputs == null ) {
            return List.of();
        }
        for ( int i = 0; i < outputs.size(); i++ ) {
            LogicalEntity output = outputs.get( i ).left;
            CheckpointMetadata meta = outputs.get( i ).right;
            if ( output == null ) {
                previews.add( UnknownType.of() );
            } else {
                TypePreview preview = switch ( meta.getDataModel() ) {
                    case RELATIONAL -> RelType.of( output.getTupleType() );
                    case DOCUMENT -> DocType.of( meta.asDoc() );
                    case GRAPH -> LpgType.of( meta.asLpg() );
                };
                previews.add( preview );
            }
        }
        return previews;
    }


    @Override
    public synchronized RelWriter createRelCheckpoint( UUID activityId, int outputIdx, AlgDataType type, boolean resetPk, @Nullable String storeName ) {
        if ( storeName == null || storeName.isEmpty() ) {
            storeName = getDefaultStore( DataModel.RELATIONAL );
        }
        if ( type.getFieldCount() == 0 ) {
            throw new IllegalArgumentException( "An output table must contain at least one column" );
        }
        AlgDataTypeField pkField = type.getFields().get( 0 ); // pk is at index 0
        if ( !StorageManager.isPkCol( pkField ) ) {
            throw new IllegalArgumentException( "The first column of an output table must be its (numeric) primary key with name " + PK_COL );
        }

        type = ActivityUtils.removeQuotesInNames( type );

        String duplicateField = findDuplicateField( type );
        if ( duplicateField != null ) {
            throw new IllegalArgumentException( "Found duplicate column in output table: " + duplicateField );
        }

        String tableName = getTableName( activityId, outputIdx );
        Transaction transaction = QueryUtils.startTransaction( transactionManager, relNamespace, "RelCreate" );

        try {
            acquireSchemaLock( transaction, relNamespace );
            ddlManager.createTable(
                    relNamespace,
                    tableName,
                    getFieldInfo( type ),
                    getPkConstraint( pkField.getName() ),
                    false,
                    List.of( getStore( storeName ) ),
                    PlacementType.AUTOMATIC,
                    transaction.createStatement() );
            transaction.commit();
        } finally {
            if ( transaction.isActive() ) {
                transaction.rollback( null );
            }
        }

        LogicalTable table = Catalog.snapshot().rel().getTable( relNamespace, tableName ).orElseThrow();
        RelMetadata meta = new RelMetadata( table.getTupleType() );
        register( activityId, outputIdx, table, meta );
        return new RelWriter( table, QueryUtils.startTransaction( transactionManager, relNamespace, "RelWrite" ), resetPk, meta );
    }


    @Override
    public synchronized DocWriter createDocCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName ) {
        if ( storeName == null || storeName.isEmpty() ) {
            storeName = getDefaultStore( DataModel.DOCUMENT );
        }

        String collectionName = getCollectionName( activityId, outputIdx );

        Transaction transaction = QueryUtils.startTransaction( transactionManager, docNamespace, "DocCreate" );
        try {
            acquireSchemaLock( transaction, docNamespace );
            ddlManager.createCollection(
                    docNamespace,
                    collectionName,
                    false,
                    List.of( getStore( storeName ) ),
                    PlacementType.AUTOMATIC,
                    transaction.createStatement()
            );
            transaction.commit();
        } finally {
            if ( transaction.isActive() ) {
                transaction.rollback( null );
            }
        }

        LogicalCollection collection = Catalog.snapshot().doc().getCollection( docNamespace, collectionName ).orElseThrow();
        DocMetadata meta = new DocMetadata();
        register( activityId, outputIdx, collection, meta );
        return new DocWriter( collection, QueryUtils.startTransaction( transactionManager, docNamespace, "DocWrite" ), meta );
    }


    @Override
    public synchronized LpgWriter createLpgCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName ) {
        if ( storeName == null || storeName.isEmpty() ) {
            storeName = getDefaultStore( DataModel.GRAPH );
        }
        String graphName = getGraphName( activityId, outputIdx );
        Transaction transaction = QueryUtils.startTransaction( transactionManager, Catalog.defaultNamespaceId, "LpgCreate" );
        // no lock required since we create a new namespace
        DataStore<?> store = getStore( storeName );
        long graphId;
        try {
            graphId = ddlManager.createGraph(
                    graphName,
                    true,
                    List.of( store ),
                    false,
                    false,
                    RuntimeConfig.GRAPH_NAMESPACE_DEFAULT_CASE_SENSITIVE.getBoolean(),
                    true,
                    transaction.createStatement()
            );
            transaction.commit();
        } finally {
            if ( transaction.isActive() ) {
                transaction.rollback( null );
            }
        }

        LogicalGraph graph = Catalog.snapshot().graph().getGraph( graphId ).orElseThrow();
        LpgMetadata meta = new LpgMetadata();
        register( activityId, outputIdx, graph, meta );
        registeredNamespaces.put( graphId, graphName );
        return new LpgWriter( graph, QueryUtils.startTransaction( transactionManager, graphId, "LpgWrite" ), meta, !BATCHABLE_LPG_ADAPTERS.contains( store.adapterName ) );
    }


    @Override
    public synchronized CheckpointWriter createCheckpoint( UUID activityId, int outputIdx, AlgDataType type, @Nullable String storeName, DataModel model ) {
        return switch ( model ) {
            case RELATIONAL -> createRelCheckpoint( activityId, outputIdx, type, true, storeName );
            case DOCUMENT -> createDocCheckpoint( activityId, outputIdx, storeName );
            case GRAPH -> createLpgCheckpoint( activityId, outputIdx, storeName );
        };
    }


    @Override
    public void dropCheckpoints( UUID activityId ) {
        for ( Entry<Integer, Pair<LogicalEntity, CheckpointMetadata>> entry : checkpoints.getOrDefault( activityId, Map.of() ).entrySet() ) {
            if ( !isLinkedCheckpoint( activityId, entry.getKey() ) ) {
                dropEntity( entry.getValue().getLeft() );
            }
            // We don't drop a linked checkpoint, as it is managed by a different StorageManager instance
        }
        checkpoints.remove( activityId );
    }


    @Override
    public void dropManagedCheckpoints( UUID activityId ) {
        Map<Integer, Pair<LogicalEntity, CheckpointMetadata>> editable = new HashMap<>( checkpoints.getOrDefault( activityId, Map.of() ) );
        for ( Entry<Integer, Pair<LogicalEntity, CheckpointMetadata>> entry : checkpoints.getOrDefault( activityId, Map.of() ).entrySet() ) {
            if ( !isLinkedCheckpoint( activityId, entry.getKey() ) ) {
                dropEntity( entry.getValue().getLeft() );
                editable.remove( entry.getKey() );
            }
            // We don't drop a linked checkpoint, as it is managed by a different StorageManager instance
        }
        if ( !editable.isEmpty() ) {
            checkpoints.put( activityId, editable );
        }
    }


    @Override
    public void dropAllCheckpoints() {
        for ( UUID activityId : checkpoints.keySet() ) {
            dropCheckpoints( activityId );
        }
    }


    @Override
    public boolean hasCheckpoint( UUID activityId, int outputIdx ) {
        return checkpoints.getOrDefault( activityId, Map.of() ).containsKey( outputIdx );
    }


    @Override
    public void linkCheckpoint( UUID activityId, int outputIdx, CheckpointReader reader ) {
        linkedCheckpoints.add( Pair.of( activityId, outputIdx ) );
        checkpoints.computeIfAbsent( activityId, k -> new ConcurrentHashMap<>() )
                .put( outputIdx, Pair.of( reader.getEntity(), reader.getMetadata() ) );
    }


    @Override
    public boolean isLinkedCheckpoint( UUID activityId, int outputIdx ) {
        return linkedCheckpoints.contains( Pair.of( activityId, outputIdx ) );
    }


    @Override
    public boolean hasAllCheckpoints( UUID activityId, int outputCount ) {
        if ( outputCount < 0 ) {
            return false;
        }
        for ( int i = 0; i < outputCount; i++ ) {
            if ( !hasCheckpoint( activityId, i ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public Transaction getTransaction( UUID activityId, CommonType commonType ) {
        return switch ( commonType ) {
            case NONE -> localTransactions.computeIfAbsent( activityId, id -> QueryUtils.startTransaction( transactionManager, Catalog.defaultNamespaceId, "LocalTx" ) );
            case EXTRACT -> extractTransaction;
            case LOAD -> loadTransaction;
        };
    }


    @Override
    public void commitTransaction( UUID activityId ) {
        if ( localTransactions.containsKey( activityId ) ) {
            localTransactions.remove( activityId ).commit();
        }
    }


    @Override
    public void rollbackTransaction( UUID activityId ) {
        if ( localTransactions.containsKey( activityId ) ) {
            localTransactions.remove( activityId ).rollback( null );
        }
    }


    @Override
    public void startCommonTransaction( @NonNull ActivityConfigModel.CommonType commonType ) {
        assert commonType != CommonType.NONE;
        if ( commonType == CommonType.EXTRACT ) {
            extractTransaction = QueryUtils.startTransaction( transactionManager, Catalog.defaultNamespaceId );
        } else if ( commonType == CommonType.LOAD ) {
            loadTransaction = QueryUtils.startTransaction( transactionManager, Catalog.defaultNamespaceId );
        }
    }


    @Override
    public void commitCommonTransaction( @NonNull ActivityConfigModel.CommonType commonType ) {
        assert commonType != CommonType.NONE;
        Transaction t = commonType == CommonType.EXTRACT ? extractTransaction : loadTransaction;
        if ( t.isActive() ) {
            t.commit();
        }
    }


    @Override
    public void rollbackCommonTransaction( @NonNull ActivityConfigModel.CommonType commonType ) {
        assert commonType != CommonType.NONE;
        Transaction t = commonType == CommonType.EXTRACT ? extractTransaction : loadTransaction;
        if ( t != null && t.isActive() ) {
            t.rollback( null );
        }
    }


    public boolean isCommonActive( @NonNull ActivityConfigModel.CommonType commonType ) {
        assert commonType != CommonType.NONE;
        Transaction t = commonType == CommonType.EXTRACT ? extractTransaction : loadTransaction;
        return t != null && t.isActive();
    }


    private LogicalEntity getCheckpoint( UUID activityId, int outputIdx ) {
        return Objects.requireNonNull( checkpoints.get( activityId ).get( outputIdx ) ).left;
    }


    private void register( UUID activityId, int outputIdx, LogicalEntity entity, CheckpointMetadata meta ) {
        checkpoints.computeIfAbsent( activityId, k -> new ConcurrentHashMap<>() )
                .put( outputIdx, Pair.of( entity, meta ) );
    }


    private DataStore<?> getStore( String storeName ) {
        return adapterManager.getStore( storeName ).orElseThrow( () -> new IllegalArgumentException( "Adapter does not exist: " + storeName ) );
    }


    private void dropEntity( LogicalEntity entity ) {
        Transaction transaction = QueryUtils.startTransaction( transactionManager, entity.getNamespaceId(), "DropCheckpoint" );
        Statement statement = transaction.createStatement();
        acquireSchemaLock( transaction, entity.getNamespaceId() );
        switch ( entity.dataModel ) {
            case RELATIONAL -> ddlManager.dropTable( (LogicalTable) entity, statement );
            case DOCUMENT -> ddlManager.dropCollection( (LogicalCollection) entity, statement );
            case GRAPH -> ddlManager.dropGraph( entity.getId(), true, statement );
        }
        transaction.commit();
    }


    private void dropNamespaces() {
        Transaction transaction = QueryUtils.startTransaction( transactionManager, relNamespace, "DropNamespaces" );
        for ( Entry<Long, String> entry : registeredNamespaces.entrySet() ) {
            acquireSchemaLock( transaction, entry.getKey() );
            ddlManager.dropNamespace( entry.getValue(), true, transaction.createStatement() );
        }
        transaction.commit();
        registeredNamespaces.clear();
    }


    private void addDefaultCheckpointAdapterIfMissing() {
        if ( Catalog.getInstance().getAdapters().values().stream().anyMatch( a -> a.uniqueName.equals( DEFAULT_CHECKPOINT_ADAPTER ) ) ) {
            STARTED_ADAPTER.set( true );
            return;
        }
        if ( STARTED_ADAPTER.compareAndSet( false, true ) ) {
            log.info( "Adding default workflow checkpoint adapter: " + DEFAULT_CHECKPOINT_ADAPTER );
            AdapterTemplate storeTemplate = AdapterManager.getAdapterTemplate( "HSQLDB", AdapterType.STORE );
            Map<String, String> settings = new HashMap<>( storeTemplate.getDefaultSettings() );
            settings.put( "trxControlMode", "locks" );
            settings.put( "type", "File" );
            settings.put( "tableType", "Cached" );
            ddlManager.createStore( DEFAULT_CHECKPOINT_ADAPTER, storeTemplate.getAdapterName(), AdapterType.STORE, settings, storeTemplate.getDefaultMode() );
        }
    }

    // Utils:


    private String findDuplicateField( AlgDataType type ) {
        Set<String> names = new HashSet<>();
        for ( AlgDataTypeField field : type.getFields() ) {
            String name = field.getName();
            if ( names.contains( name ) ) {
                return name;
            }
            names.add( name );
        }
        return null;
    }


    private List<ConstraintInformation> getPkConstraint( String pkCol ) {
        return List.of( new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( pkCol ) ) );
    }


    public static List<FieldInformation> getFieldInfo( AlgDataType tupleType ) {
        List<FieldInformation> columns = new ArrayList<>();

        int position = 0;
        for ( AlgDataTypeField field : tupleType.getFields() ) {
            FieldInformation info = new FieldInformation(
                    field.getName(),
                    getColTypeInfo( field ),
                    Collation.getDefaultCollation(),
                    null,
                    position
            );
            columns.add( info );
            position++;
        }
        return columns;
    }


    private static ColumnTypeInformation getColTypeInfo( AlgDataTypeField field ) {
        AlgDataType type = field.getType();
        boolean isArray = false;
        if ( type.getPolyType() == PolyType.ARRAY ) {
            type = type.getComponentType();
            isArray = true;
        }
        return new ColumnTypeInformation(
                type.getPolyType(),
                field.getType().getPolyType(),
                type.getRawPrecision(),
                type.getScale(),
                isArray ? (int) ((ArrayType) field.getType()).getDimension() : -1,
                isArray ? (int) ((ArrayType) field.getType()).getCardinality() : -1,
                field.getType().isNullable() );
    }


    private static void acquireSchemaLock( Transaction transaction, long namespaceId ) throws DeadlockException {
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( namespaceId ).orElse( null );
        if ( namespace == null ) {
            return; // for graphs, the namespace is already removed when the checkpoint is dropped
        }
        // in theory we would need an EXCLUSIVE lock, but this does not work if a different transaction is currently reading a checkpoint in the same namespace
        transaction.acquireLockable( LockablesRegistry.INSTANCE.getOrCreateLockable( namespace ), LockType.SHARED );
    }


    private String getTableName( UUID activityId, int outputIdx ) {
        return TABLE_PREFIX + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
    }


    private String getCollectionName( UUID activityId, int outputIdx ) {
        // TODO: change back to simplified name if collections with duplicate names in different namespaces can exist
        //return COLLECTION_PREFIX + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
        return COLLECTION_PREFIX + sessionId.toString().replace( "-", "" ) + "_"
                + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
    }


    private String getGraphName( UUID activityId, int outputIdx ) {
        return getNamespaceName( LPG_PREFIX ) + "_"
                + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
    }


    private String getNamespaceName( String prefix ) {
        return prefix + sessionId.toString().replace( "-", "" );
    }


    /**
     * In practice, calling close should not be required, since the transactions should be closed manually
     */
    @Override
    public void close() throws Exception {
        // In practice, calling close for closing transactions is not required, since the transactions should be closed manually
        assert extractTransaction == null || !extractTransaction.isActive() || extractTransaction.getNumberOfStatements() == 0 : "Common extract transaction should get explicitly committed or aborted";
        assert loadTransaction == null || !loadTransaction.isActive() || loadTransaction.getNumberOfStatements() == 0 : "Common load transaction should get explicitly committed or aborted";
        rollbackCommonTransaction( CommonType.EXTRACT );
        rollbackCommonTransaction( CommonType.LOAD );
        for ( Transaction t : localTransactions.values() ) {
            if ( t.isActive() ) {
                log.warn( "Local workflow transactions should get explicitly committed or aborted, but found active transaction {}", t.getXid() );
                t.rollback( null );
            }
        }

        dropAllCheckpoints();
        dropNamespaces();
    }


    /**
     * Removes all namespaces and associated checkpoints that currently exist.
     * This should only be called on startup to clean up anything that was not removed.
     */
    public static void clearAll( TransactionManager transactionManager ) {
        DdlManager ddlManager = DdlManager.getInstance();
        for ( LogicalNamespace ns : Catalog.snapshot().getNamespaces( null ) ) {
            Transaction transaction = QueryUtils.startTransaction( transactionManager, Catalog.defaultNamespaceId, "ClearAllCheckpoints" );
            try {
                acquireSchemaLock( transaction, ns.id );
                String name = ns.getName();
                if ( name.startsWith( REL_PREFIX ) && name.length() == REL_PREFIX.length() + 32 ) {
                    for ( LogicalTable table : transaction.getSnapshot().rel().getTablesFromNamespace( ns.id ) ) {
                        ddlManager.dropTable( table, transaction.createStatement() );
                    }
                    ddlManager.dropNamespace( name, false, transaction.createStatement() );
                } else if ( name.startsWith( DOC_PREFIX ) && name.length() == DOC_PREFIX.length() + 32 ) {
                    for ( LogicalCollection collection : transaction.getSnapshot().doc().getCollections( ns.id, null ) ) {
                        ddlManager.dropCollection( collection, transaction.createStatement() );
                    }
                    ddlManager.dropNamespace( name, false, transaction.createStatement() );

                } else if ( name.startsWith( LPG_PREFIX ) && name.length() == LPG_PREFIX.length() + 2 * 32 + 3 ) { // assumes no activity doesn't have outport with idx > 9
                    ddlManager.dropGraph( ns.id, false, transaction.createStatement() );
                }
                transaction.commit();
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( null );
                }
            }

        }

    }

}
