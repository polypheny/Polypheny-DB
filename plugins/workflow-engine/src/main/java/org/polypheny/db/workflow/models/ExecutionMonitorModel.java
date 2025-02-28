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

package org.polypheny.db.workflow.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Value;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;

@Value
public class ExecutionMonitorModel {

    String startTime; // ISO 8601:  "2025-01-17T14:30:00Z"
    long totalDuration;
    UUID targetActivity;
    List<ExecutionInfoModel> infos; // without log
    int totalCount;
    int successCount;
    int failCount;
    int skipCount;
    Map<ExecutorType, Integer> countByExecutorType;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Boolean isSuccess;

}
