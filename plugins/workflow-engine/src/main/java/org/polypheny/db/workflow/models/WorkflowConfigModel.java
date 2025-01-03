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

package org.polypheny.db.workflow.models;

import java.util.Map;
import lombok.Value;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.workflow.WorkflowManager;


@Value
public class WorkflowConfigModel {

    Map<DataModel, String> preferredStores;
    boolean fusionEnabled;
    boolean pipelineEnabled;
    int maxWorkers;
    int pipelineQueueCapacity;
    // TODO: config value for changing behavior of deleting created checkpoints


    public static WorkflowConfigModel of() {
        return new WorkflowConfigModel(
                Map.of( DataModel.RELATIONAL, WorkflowManager.DEFAULT_CHECKPOINT_ADAPTER,
                        DataModel.DOCUMENT, WorkflowManager.DEFAULT_CHECKPOINT_ADAPTER,
                        DataModel.GRAPH, WorkflowManager.DEFAULT_CHECKPOINT_ADAPTER ),
                true,
                true,
                1,
                1000
        );
    }

}