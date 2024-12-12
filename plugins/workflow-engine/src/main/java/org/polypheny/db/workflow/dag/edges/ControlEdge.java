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

package org.polypheny.db.workflow.dag.edges;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.models.EdgeModel;

@Getter
public class ControlEdge extends Edge {

    public static final int SUCCESS_PORT = 0;
    public static final int FAIL_PORT = 1;

    private final boolean onSuccess;

    @Setter
    private boolean isIgnored = false; // true if this control edge can be ignored (e.g. since target is already allowed to execute)


    public ControlEdge( ActivityWrapper from, ActivityWrapper to, boolean onSuccess ) {
        super( from, to );
        this.onSuccess = onSuccess;
    }


    public ControlEdge( ActivityWrapper from, ActivityWrapper to, int controlPort ) {
        this( from, to, controlPort == SUCCESS_PORT );
    }


    public EdgeModel toModel( boolean includeState ) {
        EdgeState state = includeState ? getState() : null;
        return new EdgeModel( from.getId(), to.getId(), getControlPort(), 0, false, state );
    }


    public int getControlPort() {
        return onSuccess ? SUCCESS_PORT : FAIL_PORT;
    }


    @Override
    public boolean isEquivalent( EdgeModel model ) {
        return hasSameEndpoints( model ) && model.isControl() && model.getFromPort() == getControlPort();
    }


    @Override
    public String toString() {
        return "ControlEdge{" +
                "onSuccess=" + onSuccess +
                ", isIgnored=" + isIgnored +
                ", from=" + from +
                ", to=" + to +
                ", edgeState=" + getState() +
                '}';
    }

}
