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
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.models.EdgeModel;

public class ControlEdge extends Edge {

    public static final int SUCCESS_PORT = 0;
    public static final int FAIL_PORT = 1;

    @Getter
    private final boolean onSuccess;


    public ControlEdge( Activity from, Activity to, boolean onSuccess ) {
        super( from, to );
        this.onSuccess = onSuccess;
    }


    public ControlEdge( Activity from, Activity to, int controlPort ) {
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

}
