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

package org.polypheny.db.workflow.engine.scheduler;

import java.util.UUID;
import lombok.Getter;
import org.polypheny.db.util.graph.AttributedDirectedGraph;
import org.polypheny.db.util.graph.DefaultEdge;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;

/**
 * Edge that stores its attributes in a list.
 */
@Getter
public class ExecutionEdge extends DefaultEdge {

    private final boolean isControl;

    // isControl == false
    private final int fromPort;
    private final int toPort;

    // isControl == true
    private final boolean onSuccess;


    public ExecutionEdge( UUID v0, UUID v1, Edge edge ) {
        super( v0, v1 );
        if ( edge instanceof DataEdge data ) {
            isControl = false;
            fromPort = data.getFromPort();
            toPort = data.getToPort();
            onSuccess = false; // value not important
        } else if ( edge instanceof ControlEdge control ) {
            isControl = true;
            onSuccess = control.isOnSuccess();

            fromPort = control.getControlPort(); // value not important
            toPort = 0; // value not important
        } else {
            throw new IllegalArgumentException( "Unexpected Edge type" );
        }
    }


    public ExecutionEdge( ExecutionEdge edge ) {
        super( edge.getSource(), edge.getTarget() );
        this.isControl = edge.isControl;
        this.fromPort = edge.fromPort;
        this.toPort = edge.toPort;
        this.onSuccess = edge.onSuccess;
    }


    public UUID getSource() {
        return (UUID) source;
    }


    public UUID getTarget() {
        return (UUID) target;
    }


    public boolean representsEdge( Edge edge ) {
        if ( edge.getFrom().getId() != getSource() || edge.getTo().getId() != getTarget() ) {
            return false;
        }
        if ( edge instanceof ControlEdge control ) {
            return isControl && control.isOnSuccess() == onSuccess;
        } else if ( edge instanceof DataEdge data ) {
            return !isControl && data.getFromPort() == fromPort && data.getToPort() == toPort;
        } else {
            throw new IllegalArgumentException( "Unexpected Edge type" );
        }
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Boolean.hashCode( isControl );
        if ( isControl ) {
            result = 31 * result + Boolean.hashCode( onSuccess );
        } else {
            result = 31 * result + Integer.hashCode( fromPort );
            result = 31 * result + Integer.hashCode( toPort );
        }
        return result;
    }


    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj instanceof ExecutionEdge e ) {
            return e.source.equals( source )
                    && e.target.equals( target )
                    && e.isControl == isControl
                    && e.onSuccess == onSuccess
                    && e.fromPort == fromPort
                    && e.toPort == toPort;
        }
        return false;
    }


    public static class ExecutionEdgeFactory implements AttributedDirectedGraph.AttributedEdgeFactory<UUID, ExecutionEdge> {

        @Override
        public ExecutionEdge createEdge( UUID v0, UUID v1, Object... attributes ) {
            if ( attributes.length != 1 ) {
                throw new IllegalArgumentException( "Invalid number of objects" );
            }

            if ( attributes[0] instanceof Edge edge ) {
                return new ExecutionEdge( v0, v1, edge );
            } else if ( attributes[0] instanceof ExecutionEdge edge ) {
                return new ExecutionEdge( edge );
            }
            throw new IllegalArgumentException( "Invalid attribute type" );
        }


        @Override
        public ExecutionEdge createEdge( UUID v0, UUID v1 ) {
            throw new UnsupportedOperationException();
        }

    }

}


