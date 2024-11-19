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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;

public class StorageManagerImpl implements StorageManager {

    public static final String REL_PREFIX = "rel_";
    public static final String DOC_PREFIX = "doc_";
    public static final String LPG_PREFIX = "lpg_";

    private final UUID sessionId;
    private final Map<DataModel, String> defaultStores;
    private final Map<UUID, Map<Integer, LogicalEntity>> checkpoints = new ConcurrentHashMap<>();
    private final AdapterManager adapterManager;
    private final TransactionManager transactionManager;
    private final DdlManager ddlManager;

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
        docNamespace = ddlManager.createNamespace( DOC_PREFIX + sessionId, DataModel.DOCUMENT, true, false, null );

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
        throw new NotImplementedException();
    }


    @Override
    public DataModel getDataModel( UUID activityId, int outputIdx ) {
        return getCheckpoint( activityId, outputIdx ).dataModel;
    }


    @Override
    public RelWriter createRelCheckpoint( UUID activityId, int outputIdx, AlgDataType type, @Nullable String storeName ) {
        if ( storeName == null ) {
            storeName = getDefaultStore( DataModel.RELATIONAL );
        }
        if ( type.getFieldCount() == 0 ) {
            throw new IllegalArgumentException( "An output table must contain at least one column" );
        }

        Transaction transaction = startTransaction( relNamespace );
        String tableName = activityId + "_" + outputIdx;
        ddlManager.createTable(
                relNamespace,
                tableName,
                getFieldInfo( type ),
                getPkConstraint( type ),
                false,
                List.of( getStore( storeName ) ),
                PlacementType.AUTOMATIC,
                transaction.createStatement() );
        transaction.commit();

        LogicalTable table = Catalog.snapshot().rel().getTable( relNamespace, tableName ).orElseThrow();
        register( activityId, outputIdx, table );
        return new RelWriter( table );
    }


    @Override
    public DocWriter createDocCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName ) {
        throw new NotImplementedException();
    }


    @Override
    public LpgWriter createLpgCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName ) {
        throw new NotImplementedException();
    }


    @Override
    public void dropCheckpoints( UUID activityId ) {
        for ( LogicalEntity entity : checkpoints.get( activityId ).values() ) {
            dropEntity( entity );
        }
    }


    @Override
    public void dropAllCheckpoints() {
        for ( UUID activityId : checkpoints.keySet() ) {
            dropCheckpoints( activityId );
        }
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
        switch ( entity.dataModel ) {
            case RELATIONAL -> ddlManager.dropTable( (LogicalTable) entity, statement );
            case DOCUMENT -> ddlManager.dropCollection( (LogicalCollection) entity, statement );
            case GRAPH -> ddlManager.dropGraph( entity.getId(), true, statement );
        }
        transaction.commit();
    }


    // Utils:
    private Transaction startTransaction( long namespace ) {
        return transactionManager.startTransaction( Catalog.defaultUserId, namespace, false, ORIGIN );
    }


    private Transaction startTransaction( LogicalEntity targetEntity ) {
        return switch ( targetEntity.dataModel ) {
            case RELATIONAL -> startTransaction( relNamespace );
            case DOCUMENT -> startTransaction( docNamespace );
            case GRAPH -> {
                throw new NotImplementedException();
            }
        };
    }


    private List<ConstraintInformation> getPkConstraint( AlgDataType tupleType ) {
        String pkColName = tupleType.getFields().get( 0 ).getName();
        return List.of( new ConstraintInformation( "PRIMARY KEY", ConstraintType.PRIMARY, List.of( pkColName ) ) );
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

}
