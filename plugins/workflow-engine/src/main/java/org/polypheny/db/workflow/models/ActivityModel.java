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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityRegistry;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;

@Value
@AllArgsConstructor
public class ActivityModel {

    String type;
    UUID id;  // activity-ID
    Map<String, JsonNode> settings; // type depends on the actual setting, given by activity type + setting key
    ActivityConfigModel config;
    RenderModel rendering;

    @JsonInclude(JsonInclude.Include.NON_NULL) // do not serialize in static version
    ActivityState state;


    public ActivityModel( String type ) {
        this( type, UUID.randomUUID() );
    }


    public ActivityModel( String type, UUID id ) {
        this( type, id, ActivityRegistry.getSerializableDefaultSettings( type ), ActivityConfigModel.of(), RenderModel.of(), null );
    }


    public ActivityModel( String type, Map<String, JsonNode> settings ) {
        this( type, UUID.randomUUID(), settings, ActivityConfigModel.of(), RenderModel.of(), null );
    }


    public ActivityModel( String type, UUID id, Map<String, JsonNode> settings, ActivityConfigModel config, RenderModel rendering ) {
        this.type = type;
        this.id = id;
        this.settings = settings;
        this.config = config;
        this.rendering = rendering;
        this.state = null;
    }

}
