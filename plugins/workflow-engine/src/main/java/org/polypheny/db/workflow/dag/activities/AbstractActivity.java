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
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;

public abstract class AbstractActivity implements Activity {

    @Getter
    private final UUID id;
    private final Map<String, JsonNode> settings;  // may contain variables that need to be replaced first
    @Getter
    @Setter
    private ActivityConfigModel config;
    @Getter
    @Setter
    private RenderModel rendering;
    @Getter
    @Setter
    private ActivityState state = ActivityState.IDLE;


    protected AbstractActivity( UUID id, Map<String, JsonNode> settings, ActivityConfigModel config, RenderModel rendering ) {
        this.id = id;
        this.settings = settings;
        this.config = config;
        this.rendering = rendering;
    }


    @Override
    public String getType() {
        // Use reflection to check if the subclass has the ActivityDefinition annotation
        ActivityDefinition annotation = this.getClass().getAnnotation( ActivityDefinition.class );

        if ( annotation == null ) {
            throw new IllegalStateException( "Class " + this.getClass().getSimpleName() +
                    " is missing the required @ActivityDefinition annotation." );
        }
        return annotation.type();
    }


    @Override
    public void updateSettings( Map<String, JsonNode> newSettings ) {
        settings.putAll( newSettings );
    }


    @Override
    public ActivityModel toModel( boolean includeState ) {
        ActivityState state = includeState ? this.state : null;
        return new ActivityModel( getType(), id, settings, config, rendering, state );
    }

}
