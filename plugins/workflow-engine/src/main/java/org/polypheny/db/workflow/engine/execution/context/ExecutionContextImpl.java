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

package org.polypheny.db.workflow.engine.execution.context;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;


public class ExecutionContextImpl implements ExecutionContext, PipeExecutionContext {
    // TODO: provide access to global variables + error msgs?

    private final StorageManager sm;
    private final ActivityWrapper activityWrapper;
    private final PortType[] remainingOutPorts; // contains the PortType for each outPort or null after the checkpoint was created to avoid duplicate checkpoints

    private volatile boolean interrupt = false;  // can be set by external thread to indicate that the activity should stop its execution
    @Getter
    private volatile double progress = 0; // 1 => 100%, can only be updated by activity thread, external thread is only allowed to read


    public ExecutionContextImpl( ActivityWrapper activityWrapper, StorageManager storageManager ) {
        this.sm = storageManager;
        this.activityWrapper = activityWrapper;
        this.remainingOutPorts = activityWrapper.getDef().getOutPortTypes();
    }


    @Override
    public void updateProgress( double value ) {
        if ( value > progress ) {
            progress = Math.min( 1, value );
        }
    }


    // Inspired by the ExecutionContext of KNIME: See its usage on https://github.com/knime/knime-examples/
    @Override
    public boolean checkInterrupted() throws Exception {
        if ( interrupt ) {
            throw new Exception( "Activity execution was interrupted" );
        }
        return interrupt;
    }


    @Override
    public RelWriter createRelWriter( int idx, AlgDataType tupleType, boolean resetPk ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.canWriteTo( PortType.REL ) ) {
            return sm.createRelCheckpoint( activityWrapper.getId(), idx, tupleType, resetPk, getStore( idx ) );
        }
        throw new IllegalArgumentException( "Unable to create a relational checkpoint for output type " + type );
    }


    @Override
    public DocWriter createDocWriter( int idx ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.canWriteTo( PortType.DOC ) ) {
            return sm.createDocCheckpoint( activityWrapper.getId(), idx, getStore( idx ) );
        }
        throw new IllegalArgumentException( "Unable to create a document checkpoint for output type " + type );
    }


    @Override
    public LpgWriter createLpgWriter( int idx ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.canWriteTo( PortType.LPG ) ) {
            return sm.createLpgCheckpoint( activityWrapper.getId(), idx, getStore( idx ) );
        }
        throw new IllegalArgumentException( "Unable to create a graph checkpoint for output type " + type );
    }


    @Override
    public CheckpointWriter createWriter( int idx, AlgDataType tupleType, boolean resetPk ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        return sm.createCheckpoint( activityWrapper.getId(), idx, tupleType, resetPk, getStore( idx ), type.getDataModel() );
    }


    @Override
    public Iterator<List<PolyValue>> getIteratorFromQuery( CheckpointQuery query, List<CheckpointReader> readers ) {
        // just like reader.getIteratorFromQuery(), but with the ability to use multiple checkpoints
        // requires a special CheckpointQuery that can specify placeholders for any one of the reader, given its index.
        // Idea for closing the iterator correctly: register it with one of the supplied readers.
        throw new NotImplementedException();
    }


    public void setInterrupted() {
        interrupt = true;
    }


    private String getStore( int idx ) {
        return activityWrapper.getConfig().getPreferredStore( idx );
    }


}
