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
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.models.EdgeModel;

@Getter
public class DataEdge extends Edge {

    private final int fromPort;
    private final int toPort;


    public DataEdge( ActivityWrapper from, ActivityWrapper to, int fromPort, int toPort ) {
        super( from, to );
        this.fromPort = fromPort;
        this.toPort = toPort;
    }


    public EdgeModel toModel( boolean includeState ) {
        EdgeState state = includeState ? getState() : null;
        return new EdgeModel( from.getId(), to.getId(), fromPort, toPort, false, state );
    }


    @Override
    public boolean isEquivalent( EdgeModel model ) {
        return hasSameEndpoints( model ) && !model.isControl() && model.getFromPort() == fromPort && model.getToPort() == toPort;
    }


    public PortType getFromPortType() {
        return from.getDef().getOutPortTypes()[fromPort];
    }


    public PortType getToPortType() {
        return to.getDef().getInPortTypes()[toPort];
    }


    public boolean isCompatible() {
        return getToPortType().canReadFrom( getFromPortType() );
    }


    @Override
    public String toString() {
        return "DataEdge{" +
                "fromPort=" + fromPort +
                ", toPort=" + toPort +
                ", from=" + from +
                ", to=" + to +
                '}';
    }

}
