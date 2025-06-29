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

package org.polypheny.db.workflow.engine.scheduler;

import java.util.Set;
import java.util.UUID;
import lombok.Value;
import org.polypheny.db.workflow.engine.execution.Executor;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.models.ActivityConfigModel.CommonType;

@Value
public class ExecutionSubmission {

    Executor executor;
    Set<UUID> activities;
    UUID rootId;
    CommonType commonType;
    UUID sessionId;
    int timeoutSeconds; // 0 for no timeout
    ExecutionInfo info;


    public String toString() {
        return "ExecutionSubmission(executor=" + this.getExecutor().getType() + ", activities=" + this.getActivities() + ", rootId=" + this.getRootId() + ", commonType=" + this.getCommonType() + ", sessionId=" + this.getSessionId() + ")";
    }

}
