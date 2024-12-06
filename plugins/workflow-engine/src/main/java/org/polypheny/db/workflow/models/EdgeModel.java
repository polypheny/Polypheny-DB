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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;

@Value
public class EdgeModel {

    UUID fromId;
    UUID toId;
    int fromPort; // for control edge: 0 on success, 1 on fail
    int toPort;

    @Getter(AccessLevel.NONE)
    boolean isControl;

    @JsonInclude(JsonInclude.Include.NON_NULL) // do not serialize EdgeState in static version
    EdgeState state;


    @JsonProperty("isControl")
    public boolean isControl() {
        return isControl;
    }


    @JsonIgnore
    public Pair<UUID, UUID> toPair() {
        return Pair.of( fromId, toId );
    }


    public static EdgeModel of( ActivityModel from, ActivityModel to ) {
        return new EdgeModel( from.getId(), to.getId(), 0, 0, false, null );
    }


    public static EdgeModel of( ActivityModel from, ActivityModel to, int toPort ) {
        return new EdgeModel( from.getId(), to.getId(), 0, toPort, false, null );
    }


    public static EdgeModel of( ActivityModel from, ActivityModel to, boolean onSuccess ) {
        return new EdgeModel( from.getId(), to.getId(), onSuccess ? ControlEdge.SUCCESS_PORT : ControlEdge.FAIL_PORT, 0, true, null );
    }

}
