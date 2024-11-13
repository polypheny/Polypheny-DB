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

import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.workflow.models.ActivityModel;

public abstract class AbstractActivity implements Activity {

    @Getter
    private final UUID id;
    private final Map<String, Object> settings; // TODO: create custom classes for settings, config, rendering
    private final Map<String, Object> config;
    private final Map<String, Object> rendering;
    @Getter
    @Setter
    private ActivityState state = ActivityState.IDLE;


    protected AbstractActivity( UUID id, Map<String, Object> settings, Map<String, Object> config, Map<String, Object> rendering ) {
        this.id = id;
        this.settings = settings;
        this.config = config;
        this.rendering = rendering;
    }


    @Override
    public ActivityModel toModel( boolean includeState ) {
        ActivityState state = includeState ? this.state : null;
        return new ActivityModel( getType(), id, settings, config, rendering, state );
    }

}
