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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
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
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DeadlockException;
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

    private final UUID sessionId;
    private final Map<DataModel, String> defaultStores;
    private final Map<UUID, Map<Integer, LogicalEntity>> checkpoints = new ConcurrentHashMap<>();
    private final Map<Long, String> registeredNamespaces = new ConcurrentHashMap<>();
    private final AdapterManager adapterManager;
    private final TransactionManager transactionManager;
    private final DdlManager ddlManager;

    private final Map<UUID, Transaction> localTransactions = new ConcurrentHashMap<>();
    private Transaction extractTransaction;
    private Transaction loadTransaction;

    private final long relNamespace;
    private final long docNamespace;


    public StorageManagerImpl( UUID sessionId, Map<DataModel, String> defaultStores ) {
        adapterManager = AdapterManager.getInstance();
        transactionManager = TransactionManagerImpl.getInstance();
        ddlManager = DdlManager.getInstance();

        this.sessionId = sessionId;
        this.defaultStores = new ConcurrentHashMap<>( defaultStores );
        String fallbackStore = Catalog.defaultStore.getAdapterName();
        this.defaultStores.putIfAbsent( DataModel.RELATIONAL, fallbackStore );
        this.defaultStores.putIfAbsent( DataModel.DOCUMENT, fallbackStore );
        this.defaultStores.putIfAbsent( DataModel.GRAPH, fallbackStore );

        relNamespace = ddlManager.createNamespace(
                REL_PREFIX + sessionId, DataModel.RELATIONAL, true, false, null );
        registeredNamespaces.put( relNamespace, REL_PREFIX + sessionId );

        docNamespace = ddlManager.createNamespace( DOC_PREFIX + sessionId, DataModel.DOCUMENT, true, false, null );
        registeredNamespaces.put( docNamespace, DOC_PREFIX + sessionId );
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
        LogicalEntity entity = Objects.requireNonNull( checkpoints.get( activityId ).get( outputIdx ) );
        return switch ( entity.dataModel ) {
            case RELATIONAL -> new RelReader( (LogicalTable) entity, QueryUtils.startTransaction( relNamespace, "RelRead" ) );
            case DOCUMENT -> new DocReader( (LogicalCollection) entity, QueryUtils.startTransaction( docNamespace, "DocRead" ) );
            case GRAPH -> new LpgReader( (LogicalGraph) entity, QueryUtils.startTransaction( entity.getNamespaceId(), "LpgRead" ) );
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
    public List<AlgDataType> getCheckpointTypes( UUID activityId ) {
        List<AlgDataType> types = new ArrayList<>();
        Map<Integer, LogicalEntity> outputs = checkpoints.get( activityId );
        if ( outputs == null ) {
            return List.of();
        }
        for ( int i = 0; i < outputs.size(); i++ ) {
            LogicalEntity output = outputs.get( i );
            types.add( output == null ? null : output.getTupleType() );
        }
        return types;
    }


    @Override
    public List<Optional<AlgDataType>> getOptionalCheckpointTypes( UUID activityId ) {
        return getCheckpointTypes( activityId ).stream().map( Optional::ofNullable ).toList();
    }


    @Override
    public RelWriter createRelCheckpoint( UUID activityId, int outputIdx, AlgDataType type, boolean resetPk, @Nullable String storeName ) {
        if ( storeName == null || storeName.isEmpty() ) {
            storeName = getDefaultStore( DataModel.RELATIONAL );
        }
        if ( type.getFieldCount() == 0 ) {
            throw new IllegalArgumentException( "An output table must contain at least one column" );
        }
        AlgDataTypeField pkField = type.getFields().get( 0 ); // pk is at index 0
        if ( resetPk ) {
            if ( !PolyType.INT_TYPES.contains( pkField.getType().getPolyType() ) ) {
                throw new IllegalArgumentException( "Only primary keys of an integer type can be reset" );
            }
        }

        Transaction transaction = QueryUtils.startTransaction( relNamespace, "RelCreate" );
        String tableName = getTableName( activityId, outputIdx );

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

        LogicalTable table = Catalog.snapshot().rel().getTable( relNamespace, tableName ).orElseThrow();

        register( activityId, outputIdx, table );
        return new RelWriter( table, QueryUtils.startTransaction( relNamespace, "RelWrite" ), resetPk );
    }


    @Override
    public synchronized DocWriter createDocCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName ) {
        if ( storeName == null || storeName.isEmpty() ) {
            storeName = getDefaultStore( DataModel.DOCUMENT );
        }

        String collectionName = getCollectionName( activityId, outputIdx );

        Transaction transaction = QueryUtils.startTransaction( docNamespace, "DocCreate" );
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

        LogicalCollection collection = Catalog.snapshot().doc().getCollection( docNamespace, collectionName ).orElseThrow();
        register( activityId, outputIdx, collection );
        return new DocWriter( collection, QueryUtils.startTransaction( docNamespace, "DocWrite" ) );
    }


    @Override
    public synchronized LpgWriter createLpgCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName ) {
        if ( storeName == null || storeName.isEmpty() ) {
            storeName = getDefaultStore( DataModel.GRAPH );
        }
        String graphName = getGraphName( activityId, outputIdx );
        Transaction transaction = QueryUtils.startTransaction( Catalog.defaultNamespaceId, "LpgCreate" );
        //acquireSchemaLock( transaction, Catalog.defaultNamespaceId ); // TODO: no lock required since we create a new namespace?
        long graphId = ddlManager.createGraph(
                graphName,
                true,
                List.of( getStore( storeName ) ),
                false,
                false,
                RuntimeConfig.GRAPH_NAMESPACE_DEFAULT_CASE_SENSITIVE.getBoolean(),
                transaction.createStatement()
        );
        transaction.commit();

        LogicalGraph graph = Catalog.snapshot().graph().getGraph( graphId ).orElseThrow();
        register( activityId, outputIdx, graph );
        registeredNamespaces.put( graphId, graphName );
        return new LpgWriter( graph, QueryUtils.startTransaction( docNamespace, "DocWrite" ) );
    }


    @Override
    public CheckpointWriter createCheckpoint( UUID activityId, int outputIdx, AlgDataType type, boolean resetPk, @Nullable String storeName, DataModel model ) {
        return switch ( model ) {
            case RELATIONAL -> createRelCheckpoint( activityId, outputIdx, type, resetPk, storeName );
            case DOCUMENT -> createDocCheckpoint( activityId, outputIdx, storeName );
            case GRAPH -> createLpgCheckpoint( activityId, outputIdx, storeName );
        };
    }


    @Override
    public void dropCheckpoints( UUID activityId ) {
        for ( LogicalEntity entity : checkpoints.getOrDefault( activityId, Map.of() ).values() ) {
            dropEntity( entity );
        }
        checkpoints.remove( activityId );
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
            case NONE -> localTransactions.computeIfAbsent( activityId, id -> QueryUtils.startTransaction( Catalog.defaultNamespaceId, "LocalTx" ) );
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
        // TODO: call this method at the correct time
        assert commonType != CommonType.NONE;
        if ( commonType == CommonType.EXTRACT ) {
            extractTransaction = QueryUtils.startTransaction( Catalog.defaultNamespaceId );
        } else if ( commonType == CommonType.LOAD ) {
            loadTransaction = QueryUtils.startTransaction( Catalog.defaultNamespaceId );
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
        return Objects.requireNonNull( checkpoints.get( activityId ).get( outputIdx ) );
    }


    private void register( UUID activityId, int outputIdx, LogicalEntity entity ) {
        checkpoints.computeIfAbsent( activityId, k -> new ConcurrentHashMap<>() )
                .put( outputIdx, entity );
    }


    private DataStore<?> getStore( String storeName ) {
        return adapterManager.getStore( storeName ).orElseThrow( () -> new IllegalArgumentException( "Adapter does not exist: " + storeName ) );
    }


    private void dropEntity( LogicalEntity entity ) {
        Transaction transaction = startTransaction( entity );
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
        Transaction transaction = QueryUtils.startTransaction( relNamespace, "DropNamespaces" );
        for ( Entry<Long, String> entry : registeredNamespaces.entrySet() ) {
            acquireSchemaLock( transaction, entry.getKey() );
            ddlManager.dropNamespace( entry.getValue(), true, transaction.createStatement() );
        }
        transaction.commit();
        registeredNamespaces.clear();
    }

    // Utils:


    private Transaction startTransaction( LogicalEntity targetEntity ) {
        return switch ( targetEntity.dataModel ) {
            case RELATIONAL -> QueryUtils.startTransaction( relNamespace );
            case DOCUMENT -> QueryUtils.startTransaction( docNamespace );
            case GRAPH -> {
                throw new NotImplementedException();
            }
        };
    }


    private List<ConstraintInformation> getPkConstraint( String pkCol ) {
        return List.of( new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( pkCol ) ) );
    }


    private List<FieldInformation> getFieldInfo( AlgDataType tupleType ) {
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


    private ColumnTypeInformation getColTypeInfo( AlgDataTypeField field ) {
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


    private void acquireSchemaLock( Transaction transaction, long namespaceId ) throws DeadlockException {
        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).orElseThrow();
        transaction.acquireLockable( LockablesRegistry.INSTANCE.getOrCreateLockable( namespace ), LockType.EXCLUSIVE );
    }


    private String getTableName( UUID activityId, int outputIdx ) {
        return TABLE_PREFIX + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
    }


    private String getCollectionName( UUID activityId, int outputIdx ) {
        return COLLECTION_PREFIX + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
    }


    private String getGraphName( UUID activityId, int outputIdx ) {
        return LPG_PREFIX + sessionId.toString().replace( "-", "" ) + "_"
                + activityId.toString().replace( "-", "" ) + "_" + outputIdx;
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
            assert !t.isActive() : "local transactions should get explicitly committed or aborted";
            t.rollback( null );
        }

        dropAllCheckpoints();
        dropNamespaces();

    }

}
