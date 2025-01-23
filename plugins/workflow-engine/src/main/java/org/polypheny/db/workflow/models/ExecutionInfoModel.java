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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo.ExecutionState;

@Value
public class ExecutionInfoModel {

    String submissionTime; // ISO 8601: "2025-01-17T14:30:00Z"
    long totalDuration;
    Map<ExecutionState, Long> durations;
    List<UUID> activities;
    UUID root;
    ExecutorType executorType;
    ExecutionState state;

    @Getter(AccessLevel.NONE)
    boolean isSuccess; // only valid when state == DONE
    long tuplesWritten;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<String> log;


    @JsonProperty("isSuccess")
    public boolean isSuccess() {
        return isSuccess;
    }

}
