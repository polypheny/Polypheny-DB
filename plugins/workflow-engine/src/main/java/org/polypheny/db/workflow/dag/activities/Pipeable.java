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

package org.polypheny.db.workflow.dag.activities;

import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointOutputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

// TODO: write test to ensure at most 1 output was specified
public interface Pipeable extends Activity {

    /**
     * Whether this activity instance can be piped, given the input tuple types and setting values.
     * If no final decision can be made yet, an empty optional is returned.
     * If this method is overridden, it is required to also provide a custom execute implementation.
     * This is necessary, as it might be used in the case that this activity cannot be piped.
     *
     * @param inTypes preview of the input types. For inactive edges, the entry {@link org.polypheny.db.workflow.dag.activities.TypePreview.InactiveType} (important for non-default DataStateMergers).
     * @param settings preview of the settings
     * @return an Optional containing the final decision whether this activity can be piped, or an empty Optional if it cannot be stated at this point
     */
    default Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return Optional.of( true );
    }

    /**
     * Default implementation that uses the {@code pipe()} method for executing this activity without a PipeExecutor.
     * It is always desirable to override this method with a custom implementation that does not depend on {@code pipe()}.
     * An issue of this implementation is that it does not check the ExecutionContext for interrupts -> no early return in case of an interrupt!
     *
     * @param inputs a list of input readers for each input specified by the annotation. For inactive edges, the entry is null (important for non-default DataStateMergers).
     * @param settings the instantiated setting values, according to the specified settings annotations
     * @param ctx ExecutionContext to be used for creating checkpoints, updating progress and periodically checking for an abort
     * @throws Exception in case the execution fails or is interrupted at any point
     */
    @Override
    default void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        List<AlgDataType> inputTypes = inputs.stream().map( i -> i == null ? null : i.getTupleType() ).toList();
        assert canPipe(
                inputTypes.stream().map( TypePreview::ofType ).toList(),
                SettingsPreview.of( settings )
        ).orElseThrow() : "Cannot use the default execute implementation of Pipeable if canPipe returns false.";

        ctx.logInfo( "Relying on the pipeable implementation of " + getClass().getSimpleName() + " to execute the activity." );

        AlgDataType type = lockOutputType( inputTypes, settings );
        List<InputPipe> inPipes = inputs.stream().map( reader -> reader == null ? null :
                (InputPipe) new CheckpointInputPipe( reader ) ).toList();

        PipeExecutionContext pipeCtx = (ExecutionContextImpl) ctx;

        List<Long> inCounts = inputs.stream().map( reader -> reader == null ? null : reader.getTupleCount() ).toList();
        long tupleCount = estimateTupleCount( inputTypes, settings, inCounts, pipeCtx::getTransaction );
        CheckpointWriter writer = ctx.createWriter( 0, type );

        try ( OutputPipe outPipe = new CheckpointOutputPipe( type, writer, ctx, tupleCount ) ) {
            pipe( inPipes, outPipe, settings, pipeCtx );
        } catch ( PipeInterruptedException e ) {
            ctx.throwException( "Activity execution was interrupted" );
        } finally {
            for ( InputPipe inputPipe : inPipes ) {
                if ( inputPipe instanceof CheckpointInputPipe closeable ) {
                    try {
                        closeable.close();
                    } catch ( Exception ignored ) {
                    }
                }
            }
        }
    }

    /**
     * Define the output type of this pipe.
     * Afterward, it may no longer be changed until reset() is called.
     *
     * @param inTypes the types of the input pipes. For inactive edges, the entry is null (important for non-default DataStateMergers).
     * @param settings the resolved settings
     * @return the compulsory output type of this instance until the next call to reset(), or null if this activity has no outputs.
     */
    AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception;

    /**
     * Successively consumes the tuples of the input pipe(s) and forwards produced tuples to the output pipe.
     * There does not have to be a 1:1 relationship between input and output tuples.
     * The Pipeable activity is not expected to close the output themselves. This is done by the executor
     * after the pipe method returns.
     *
     * @param inputs the InputPipes to iterate over. For inactive edges, the pipe is null (important for non-default DataStateMergers).
     * @param output the output pipe for sending output tuples to that respect the locked output type, or null if this activity has no output
     * @param settings the resolved settings
     * @param ctx ExecutionContext to be used for access to the transaction (interrupt checking is done automatically by the pipes)
     * @throws PipeInterruptedException if thread gets interrupted during execution (used for prematurely stopping execution)
     * @throws Exception if some other problem occurs during execution that requires the execution of the pipe to stop
     */
    void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception;

    /**
     * Signals all inputs that no more tuples will be consumed by this Pipeable.
     * It is important ensure that this holds. The easiest way to do this is by returning from {@code pipe()} directly after calling this method.
     *
     * @param inputs all input pipes
     */
    default void finish( List<InputPipe> inputs ) {
        inputs.forEach( InputPipe::finishIteration );
    }


    class PipeInterruptedException extends RuntimeException {

        public PipeInterruptedException() {
            this( null );
        }


        // Constructor that accepts a cause
        public PipeInterruptedException( Throwable cause ) {
            super( "The pipe operation was interrupted.", cause );
        }

    }

}
