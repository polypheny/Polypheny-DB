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

/**
 * A StorageManager is responsible for managing the checkpoints of a specific session.
 * It is NOT responsible to enforce compliance of the checkpoint data model with the corresponding ActivityDef.
 */
public interface StorageManager {
    String ORIGIN = "WorkflowEngine";
    String PK_COL = "key";

    UUID getSessionId();

    String getDefaultStore( DataModel model );

    void setDefaultStore( DataModel model, String storeName );

    CheckpointReader readCheckpoint( UUID activityId, int outputIdx );

    DataModel getDataModel( UUID activityId, int outputIdx );

    RelWriter createRelCheckpoint( UUID activityId, int outputIdx, AlgDataType type, @Nullable String storeName );

    DocWriter createDocCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName );

    LpgWriter createLpgCheckpoint( UUID activityId, int outputIdx, @Nullable String storeName );

    void dropCheckpoints( UUID activityId );

    void dropAllCheckpoints();

}
