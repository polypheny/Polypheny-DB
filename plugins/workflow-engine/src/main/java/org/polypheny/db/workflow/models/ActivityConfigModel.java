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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;


@Value
public class ActivityConfigModel {

    boolean enforceCheckpoint;
    int timeoutSeconds; // 0 for no timeout
    String[] preferredStores;  // one entry per output

    @JsonProperty(required = true)
    CommonType commonType;

    ControlStateMerger controlStateMerger;
    /**
     * Used to determine the successful execution of the entire workflow.
     * The default is ANY, resulting in a workflow to be unable to fail, given that execution terminates eventually.
     */
    ExpectedOutcome expectedOutcome;


    /**
     * Returns the name of the preferred store corresponding to the given output index
     * or null if no preferred store is configured for that output.
     *
     * @param outputIdx the index of the output in question
     * @return name of the preferred DataStore for that output or null if the default store should be used.
     */
    @JsonIgnore
    public String getPreferredStore( int outputIdx ) {
        if ( preferredStores == null || preferredStores.length <= outputIdx ) {
            return null;
        }
        return preferredStores[outputIdx];
    }


    @JsonCreator
    public ActivityConfigModel(
            @JsonProperty("enforceCheckpoint") boolean enforceCheckpoint,
            @JsonProperty("timeoutSeconds") int timeoutSeconds,
            @JsonProperty("preferredStores") String[] preferredStores,
            @JsonProperty(value = "commonType", required = true) CommonType commonType,
            @JsonProperty("controlStateMerger") ControlStateMerger controlStateMerger,
            @JsonProperty("expectedOutcome") ExpectedOutcome expectedOutcome ) {

        this.enforceCheckpoint = enforceCheckpoint;
        this.timeoutSeconds = timeoutSeconds;
        this.preferredStores = preferredStores;
        this.commonType = commonType;
        this.controlStateMerger = controlStateMerger;
        this.expectedOutcome = expectedOutcome != null ? expectedOutcome : ExpectedOutcome.ANY; // Default handling
    }


    public static ActivityConfigModel of() {
        return new ActivityConfigModel( false, 0, null, CommonType.NONE, ControlStateMerger.AND_AND, ExpectedOutcome.ANY );
    }


    public enum CommonType {
        NONE,
        EXTRACT,
        LOAD
    }


    public enum ExpectedOutcome {
        MUST_SUCCEED,
        MUST_FAIL,
        ANY
    }

}
