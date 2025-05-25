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
    private final int toPort; // for multi InPorts: does not have to be < inPorts.length
    private final boolean isMulti;


    public DataEdge( ActivityWrapper from, ActivityWrapper to, int fromPort, int toPort ) {
        super( from, to );
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.isMulti = to.getDef().getInPort( toPort ).isMulti();
    }


    public static DataEdge of( DataEdge nextEdge, int toPort ) {
        DataEdge edge = new DataEdge( nextEdge.getFrom(), nextEdge.getTo(), nextEdge.getFromPort(), toPort );
        edge.setState( nextEdge.getState() );
        return edge;
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
        return from.getDef().getOutPortType( fromPort );
    }


    public PortType getToPortType() {
        return to.getDef().getInPortType( toPort );
    }


    public boolean isCompatible() {
        return getFromPortType().couldBeCompatibleWith( getToPortType() ); // only detects static incompatibilities, ANY is always okay
    }


    public boolean isFirstMulti() {
        return isMulti && toPort == to.getDef().getInPorts().length - 1;
    }


    public int getMultiIndex() {
        if ( !isMulti ) {
            throw new IllegalStateException( "Edge is not a multi-edge." );
        }
        return toPort + 1 - to.getDef().getInPorts().length;
    }


    @Override
    public String toString() {
        return "DataEdge{" +
                "fromPort=" + fromPort +
                ", toPort=" + toPort +
                ", from=" + from +
                ", to=" + to +
                ", edgeState=" + getState() +
                '}';
    }

}
