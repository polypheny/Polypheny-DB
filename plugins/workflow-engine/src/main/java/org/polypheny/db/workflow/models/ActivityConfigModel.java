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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.Activity.ControlStateMerger;


// Available config keys are determined by activity category (e.g. extract activity and transform can enforce checkpoint, load can't)
@Value
public class ActivityConfigModel {

    boolean enforceCheckpoint;
    String[] preferredStores;  // one entry per output

    @JsonProperty(required = true)
    CommonType commonType;

    ControlStateMerger controlStateMerger;


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


    public static ActivityConfigModel of() {
        return new ActivityConfigModel( false, null, CommonType.NONE, ControlStateMerger.AND_AND );
    }


    public enum CommonType {
        // TODO: scheduler must take EXTRACT and LOAD "transactions" into account (these activities are executed first / last and succeed or fail atomically). LOAD can only start when EXTRACT has committed.
        // Any predecessor of a EXTRACT must also be a EXTRACT
        // Any successor of a LOAD must also be a LOAD
        NONE,
        EXTRACT, // only relevant for EXTRACT + TRANSFORM category activities
        LOAD // only relevant for LOAD + TRANSFORM category activities
    }

}