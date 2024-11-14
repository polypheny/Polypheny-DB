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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingValue;
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

    boolean validate( List<AlgDataType> inSchemas );

    List<AlgDataType> computeOutSchemas( List<AlgDataType> inSchemas );

    void execute(); // default execution method. TODO: introduce execution context to track progress, abort, inputs, outputs...

    void updateSettings( Map<String, SettingValue> settings );

    ActivityConfigModel getConfig();

    void setConfig( ActivityConfigModel config );

    RenderModel getRendering();

    void setRendering( RenderModel rendering );

    ActivityModel toModel( boolean includeState );

    static Activity fromModel( ActivityModel model ) {
        throw new NotImplementedException();
    }

    enum PortType {
        ANY,
        REL,
        DOC,
        LPG
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
