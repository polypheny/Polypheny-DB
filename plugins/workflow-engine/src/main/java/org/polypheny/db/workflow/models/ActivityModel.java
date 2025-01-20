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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
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

    // the following fields are not serialized in the static version -> not used for deserializing an ActivityWrapper
    @JsonInclude(JsonInclude.Include.NON_NULL)
    ActivityState state;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<TypePreviewModel> inTypePreview;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<TypePreviewModel> outTypePreview;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    String invalidReason; // null if not invalid
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Map<String, String> invalidSettings; // null if not invalid
    @JsonInclude(JsonInclude.Include.NON_NULL)
    Map<String, JsonNode> variables;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    ExecutionInfoModel executionInfo;


    public ActivityModel( String type ) {
        this( type, RenderModel.of() );
    }


    public ActivityModel( String type, RenderModel renderModel ) {
        this( type, UUID.randomUUID(), ActivityRegistry.getSerializableDefaultSettings( type ), ActivityConfigModel.of(), renderModel );
    }


    public ActivityModel( String type, Map<String, JsonNode> settings ) {
        this( type, UUID.randomUUID(), settings, ActivityConfigModel.of(), RenderModel.of() );
    }


    public ActivityModel( String type, UUID id, Map<String, JsonNode> settings, ActivityConfigModel config, RenderModel rendering ) {
        this.type = type;
        this.id = id;
        this.settings = settings;
        this.config = config;
        this.rendering = rendering;
        this.state = null;
        this.inTypePreview = null;
        this.outTypePreview = null;
        this.invalidReason = null;
        this.invalidSettings = null;
        this.variables = null;
        this.executionInfo = null;
    }


    public ActivityModel createCopy( double posX, double posY ) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            ActivityModel trueCopy = mapper.readValue( mapper.writeValueAsString( this ), ActivityModel.class ); // but we need a copy with a different UUID...
            return new ActivityModel( trueCopy.type,
                    UUID.randomUUID(),
                    trueCopy.settings,
                    trueCopy.config,
                    new RenderModel( posX, posY, trueCopy.rendering.getName(), trueCopy.rendering.getNotes() ) );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }

}
