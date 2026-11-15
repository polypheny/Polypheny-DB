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

package org.polypheny.db.workflow.models.responses;

import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.models.ExecutionInfoModel;

@Value
public class ActivityStatsResponse {

    ActivityState state;
    boolean rolledBack;
    ExecutionInfoModel executionInfo;


    public ActivityStatsResponse( ActivityWrapper wrapper ) {
        this.state = wrapper.getState();
        this.rolledBack = wrapper.isRolledBack();
        ExecutionInfo info = wrapper.getExecutionInfo();
        this.executionInfo = info == null ? null : info.toModel( false );
    }

}
