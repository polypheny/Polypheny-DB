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

package org.polypheny.db.workflow.engine.execution;

import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.engine.storage.DocWriter;
import org.polypheny.db.workflow.engine.storage.LpgWriter;
import org.polypheny.db.workflow.engine.storage.RelWriter;
import org.polypheny.db.workflow.engine.storage.StorageManager;


public class ExecutionContext {

    private StorageManager sm;
    private Activity activity;
    private PortType[] remainingOutPorts; // contains the PortType for each outPort or null after the checkpoint was created to avoid duplicate checkpoints
    private ReadableVariableStore variables; // TODO: only store global variables + error msgs?
    private volatile boolean interrupt = false;  // can be set by external thread to indicate that the activity should stop its execution
    @Getter
    private volatile double progress = 0; // 1 => 100%, can only be updated by activity thread, external thread is only allowed to read


    public ExecutionContext( Activity activity, ReadableVariableStore variables ) {
        this.activity = activity;
        this.remainingOutPorts = activity.getDef().getOutPortTypes();
        this.variables = variables;
    }


    public void updateProgress( double value ) {
        if ( value > progress ) {
            progress = Math.min( 1, value );
        }
    }


    // Inspired by the ExecutionContext of KNIME: See its usage on https://github.com/knime/knime-examples/
    public boolean checkInterrupted() throws Exception {
        if ( interrupt ) {
            throw new Exception( "Activity execution was interrupted" );
        }
        return interrupt;
    }


    public void setInterrupted() {
        interrupt = true;
    }


    public RelWriter createRelWriter( int idx, AlgDataType tupleType ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.canWriteTo( PortType.REL ) ) {
            return sm.createRelCheckpoint( activity.getId(), idx, tupleType );
        }
        throw new IllegalArgumentException( "Unable to create a relational checkpoint for output type " + type );
    }


    public DocWriter createDocWriter( int idx ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.canWriteTo( PortType.DOC ) ) {
            return sm.createDocCheckpoint( activity.getId(), idx );
        }
        throw new IllegalArgumentException( "Unable to create a document checkpoint for output type " + type );
    }


    public LpgWriter createLpgWriter( int idx ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.canWriteTo( PortType.LPG ) ) {
            return sm.createLpgCheckpoint( activity.getId(), idx );
        }
        throw new IllegalArgumentException( "Unable to create a graph checkpoint for output type " + type );
    }


}
