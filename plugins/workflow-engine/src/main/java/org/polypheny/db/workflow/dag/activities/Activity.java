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

package org.polypheny.db.workflow.dag.activities;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;

public interface Activity {

    String getType();

    UUID getId();

    ActivityState getState();

    /**
     * Changes the state of this activity to the specified state.
     * After initialization, this should never be done by the activity itself.
     * The state is typically changed by the scheduler.
     *
     * @param state the new state of this activity
     */
    void setState( ActivityState state );

    /**
     * This method computes the output tuple-types by considering (a preview of) input types and settings.
     * If the input types or settings are not available or cannot be validated yet, the output type is set to an empty {@link Optional}
     * for the outputs that depend on it. The same holds for outputs whose type can only be determined during execution.
     * If any available setting or input type results in a contradiction or invalid state,
     * an {@link ActivityException} is thrown.
     *
     * @param inTypes a list of {@link Optional<AlgDataType>} representing the input tuple types.
     * @param settings a map of setting keys to {@link Optional<SettingValue>} representing the available settings.
     * @return a list of {@link Optional<AlgDataType>} representing the expected output tuple types.
     * If an output type cannot be determined at this point, the corresponding {@link Optional} will be empty.
     * @throws ActivityException if any available setting or input type results in a contradiction or invalid state.
     */
    List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException;

    void execute( List<CheckpointReader> inputs, ExecutionContext ctx ) throws Exception; // default execution method. TODO: introduce execution context to track progress, abort, inputs, outputs...

    void updateSettings( Map<String, JsonNode> settings );

    ActivityConfigModel getConfig();

    void setConfig( ActivityConfigModel config );

    RenderModel getRendering();

    void setRendering( RenderModel rendering );

    ActivityDef getDef();

    /**
     * Reset any execution-specific state of this activity.
     * It is guaranteed to be called before execution starts.
     */
    void reset();

    ActivityModel toModel( boolean includeState );

    static Activity fromModel( ActivityModel model ) {
        return ActivityRegistry.activityFromModel( model );
    }

    enum PortType {
        ANY,
        REL,
        DOC,
        LPG;


        public boolean canReadFrom( PortType other ) {
            return this == other || this == ANY;
        }


        public boolean canWriteTo( PortType other ) {
            return this == other || other == ANY;
        }
    }


    enum ActivityState {
        IDLE,
        QUEUED,
        EXECUTING,
        SKIPPED,  // => execution was aborted
        FAILED,
        FINISHED,
        SAVED  // => finished + checkpoint created
    }


    enum ActivityCategory {
        EXTRACT,
        TRANSFORM,
        LOAD
        // more granular categories are also thinkable
    }

}
