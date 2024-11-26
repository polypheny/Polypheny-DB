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
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper.ActivityState;

@Value
public class ActivityModel {

    String type;
    UUID id;  // activity-ID
    Map<String, JsonNode> settings; // type depends on the actual setting, given by activity type + setting key
    ActivityConfigModel config;
    RenderModel rendering;

    @JsonInclude(JsonInclude.Include.NON_NULL) // do not serialize in static version
    ActivityState state;

}