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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityWrapper;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo.LogLevel;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;


public class ExecutionContextImpl implements ExecutionContext, PipeExecutionContext, AutoCloseable {

    private final StorageManager sm;
    private final ActivityWrapper activityWrapper;
    private final ExecutionInfo info;
    private final PortType[] remainingOutPorts; // contains the PortType for each outPort or null after the checkpoint was created to avoid duplicate checkpoints
    private final List<CheckpointWriter> writers = new ArrayList<>();
    private final List<Long> inCounts; // a PipeExecutionContext provides access to estimated number of input tuples

    private volatile boolean interrupt = false;  // can be set by external thread to indicate that the activity should stop its execution


    public ExecutionContextImpl( ActivityWrapper activityWrapper, StorageManager storageManager, ExecutionInfo info ) {
        this( activityWrapper, storageManager, info, null );
    }


    public ExecutionContextImpl( ActivityWrapper activityWrapper, StorageManager storageManager, ExecutionInfo info, @Nullable List<Long> inCounts ) {
        this.sm = storageManager;
        this.activityWrapper = activityWrapper;
        this.info = info;
        this.remainingOutPorts = activityWrapper.getDef().getOutPortTypes();
        this.inCounts = inCounts;
    }


    @Override
    public void updateProgress( double value ) {
        info.setProgress( activityWrapper.getId(), value );
    }


    // Inspired by the ExecutionContext of KNIME: See its usage on https://github.com/knime/knime-examples/
    @Override
    public void checkInterrupted() throws ExecutorException {
        if ( interrupt ) {
            throwException( "Activity execution was interrupted" );
        }
    }


    @Override
    public RelWriter createRelWriter( int idx, AlgDataType tupleType, boolean resetPk ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.couldBeCompatibleWith( PortType.REL ) ) {
            RelWriter writer = sm.createRelCheckpoint( activityWrapper.getId(), idx, tupleType, resetPk, getStore( idx ) );
            writers.add( writer );
            return writer;
        }
        throw new IllegalArgumentException( "Unable to create a relational checkpoint for output type " + type );
    }


    @Override
    public DocWriter createDocWriter( int idx ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.couldBeCompatibleWith( PortType.DOC ) ) {
            DocWriter writer = sm.createDocCheckpoint( activityWrapper.getId(), idx, getStore( idx ) );
            writers.add( writer );
            return writer;
        }
        throw new IllegalArgumentException( "Unable to create a document checkpoint for output type " + type );
    }


    @Override
    public LpgWriter createLpgWriter( int idx ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        if ( type.couldBeCompatibleWith( PortType.LPG ) ) {
            LpgWriter writer = sm.createLpgCheckpoint( activityWrapper.getId(), idx, getStore( idx ) );
            writers.add( writer );
            return writer;
        }
        throw new IllegalArgumentException( "Unable to create a graph checkpoint for output type " + type );
    }


    @Override
    public CheckpointWriter createWriter( int idx, AlgDataType tupleType, boolean resetPk ) {
        PortType type = Objects.requireNonNull( remainingOutPorts[idx] );
        remainingOutPorts[idx] = null;
        CheckpointWriter writer = sm.createCheckpoint( activityWrapper.getId(), idx, tupleType, resetPk, getStore( idx ), type.getDataModel( tupleType ) );
        writers.add( writer );
        return writer;
    }


    @Override
    public Transaction getTransaction() {
        if ( !activityWrapper.getDef().hasCategory( ActivityCategory.EXTRACT )
                && !activityWrapper.getDef().hasCategory( ActivityCategory.LOAD )
                && !(activityWrapper.getActivity() instanceof Fusable) ) {
            throw new IllegalStateException( "Only EXTRACT or LOAD or fusable activities have access to transactions" );
        }
        return sm.getTransaction( activityWrapper.getId(), activityWrapper.getConfig().getCommonType() );
    }


    @Override
    public List<Long> getEstimatedInCounts() {
        assert inCounts != null;
        return inCounts;
    }


    @Override
    public ReadableVariableStore getVariableStore() {
        return activityWrapper.getVariables();
    }


    @Override
    public void throwException( String message ) throws ExecutorException {
        throw new ExecutorException( message, activityWrapper.getId() );
    }


    @Override
    public void throwException( Throwable cause ) throws ExecutorException {
        throw new ExecutorException( cause, activityWrapper.getId() );
    }


    @Override
    public void logInfo( String message ) {
        info.appendLog( activityWrapper.getId(), LogLevel.INFO, message );
    }


    @Override
    public void logWarning( String message ) {
        info.appendLog( activityWrapper.getId(), LogLevel.WARN, message );

    }


    @Override
    public void logError( String message ) {
        info.appendLog( activityWrapper.getId(), LogLevel.ERROR, message );
    }


    public void setInterrupted() {
        interrupt = true;
    }


    private String getStore( int idx ) {
        return activityWrapper.getConfig().getPreferredStore( idx );
    }


    @Override
    public void close() throws Exception {
        List<Exception> exceptions = new ArrayList<>();
        long writeCount = 0;
        for ( CheckpointWriter writer : writers ) {
            try {
                writer.close();
                writeCount += writer.getWriteCount();
            } catch ( Exception e ) {
                exceptions.add( e );
            }
        }
        if ( !writers.isEmpty() ) { // do not set the count for PipeExecutionContexts and activities without outputs
            info.setTuplesWritten( writeCount );
        }
        if ( !exceptions.isEmpty() ) {
            throw exceptions.get( 0 ); // we only throw the first exception
        }
    }

}
