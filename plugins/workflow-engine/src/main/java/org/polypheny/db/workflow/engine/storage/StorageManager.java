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

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;

/**
 * A StorageManager is responsible for managing the checkpoints of a specific session.
 * It is NOT responsible to enforce compliance of the checkpoint data model with the corresponding ActivityDef.
 */
public interface StorageManager extends AutoCloseable { // TODO: remove AutoCloseable when transactions are managed by individual readers / writers

    String ORIGIN = "WorkflowEngine";
    String PK_COL = "key";
    AlgDataTypeField PK_FIELD = new AlgDataTypeFieldImpl( null, PK_COL, 0, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.BIGINT ) );

    UUID getSessionId();

    String getDefaultStore( DataModel model );

    void setDefaultStore( DataModel model, String storeName );

    CheckpointReader readCheckpoint( UUID activityId, int outputIdx );

    DataModel getDataModel( UUID activityId, int outputIdx );

    AlgDataType getTupleType( UUID activityId, int outputIdx );

    List<AlgDataType> getCheckpointTypes( UUID activityId );

    List<TypePreview> getCheckpointPreviewTypes( UUID activityId );

    /**
     * Creates a relational checkpoint for an activity output and returns a RelWriter for that checkpoint.
     *
     * @param activityId the unique identifier of the activity.
     * @param outputIdx the index of the output to checkpoint.
     * @param type the schema of the output table.
     * @param resetPk whether to reset the primary key (allowed only for single integer-type keys).
     * @param storeName the name of the store where the table will be created; if null or empty the default relational store of the workflow is used.
     * @return a {@link RelWriter} for writing data to the checkpoint.
     * @throws IllegalArgumentException if any of the required conditions is not met.
     */
    RelWriter createRelCheckpoint( UUID activityId, int outputIdx, AlgDataType type, boolean resetPk, @Nullable String storeName );

    DocWriter createDocCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName );

    LpgWriter createLpgCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName );

    CheckpointWriter createCheckpoint( UUID activityId, int outputIdx, AlgDataType type, boolean resetPk, @Nullable String storeName, DataModel model );

    void dropCheckpoints( UUID activityId );

    void dropAllCheckpoints();

    boolean hasCheckpoint( UUID activityId, int outputIdx );

    boolean hasAllCheckpoints( UUID activityId, int outputCount );

    /**
     * Returns a transaction to be used by the specified activity for extracting or loading data stored in this Polypheny instance (excluding checkpoints).
     * Each activity has at most 1 such transaction at any point. If commonType != NONE, the transaction is shared between multiple activities
     * and only committed when all of these activities finish their execution successfully.
     * The returned activity must not be committed or aborted. This should only be done by the scheduler, using an appropriate method of this interface.
     *
     * @param activityId the activity for which the transaction is returned
     * @param commonType whether to return a common transaction for the specified common type or return a transaction only for this activity
     * @return the transaction for the activity
     */
    Transaction getTransaction( UUID activityId, CommonType commonType );

    /**
     * If the activity has any active extract or load transaction associated with it (excluding common transactions) it will be committed.
     * This method should only be called after the activity has terminated its execution.
     *
     * @param activityId the activity whose associated extract or load transaction will be committed if it exists.
     */
    void commitTransaction( UUID activityId );

    /**
     * If the activity has any active extract or load transaction associated with it (excluding common transactions) it will be aborted.
     * This method should only be called after the activity has terminated its execution.
     *
     * @param activityId the activity whose associated extract or load transaction will be aborted if it exists.
     */
    void rollbackTransaction( UUID activityId );

    /**
     * Starts a common EXTRACT or LOAD transaction.
     */
    void startCommonTransaction( @NonNull ActivityConfigModel.CommonType commonType );

    /**
     * Commits the common transaction of the given type (either EXTRACT or LOAD).
     * Any activity that uses this transaction must have terminated its execution before this method should be called.
     * <p>
     * This method is NOT thread safe. It should only be called by the scheduler and at most once.
     *
     * @param commonType which common transaction to commit
     */
    void commitCommonTransaction( @NonNull ActivityConfigModel.CommonType commonType );

    /**
     * Aborts the common transaction of the given type (either EXTRACT or LOAD).
     * Any activity that uses this transaction must have terminated its execution before this method should be called.
     * <p>
     * This method is NOT thread safe. It should only be called by the scheduler and at most once.
     *
     * @param commonType which common transaction to roll back
     */
    void rollbackCommonTransaction( @NonNull ActivityConfigModel.CommonType commonType );

    boolean isCommonActive( @NonNull ActivityConfigModel.CommonType commonType );

}
