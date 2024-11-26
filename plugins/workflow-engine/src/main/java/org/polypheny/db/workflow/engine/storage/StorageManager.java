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

import java.util.UUID;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

/**
 * A StorageManager is responsible for managing the checkpoints of a specific session.
 * It is NOT responsible to enforce compliance of the checkpoint data model with the corresponding ActivityDef.
 */
public interface StorageManager extends AutoCloseable { // TODO: remove AutoCloseable when transactions are managed by individual readers / writers

    String ORIGIN = "WorkflowEngine";
    String PK_COL = "key";

    UUID getSessionId();

    String getDefaultStore( DataModel model );

    void setDefaultStore( DataModel model, String storeName );

    CheckpointReader readCheckpoint( UUID activityId, int outputIdx );

    DataModel getDataModel( UUID activityId, int outputIdx );

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

}
