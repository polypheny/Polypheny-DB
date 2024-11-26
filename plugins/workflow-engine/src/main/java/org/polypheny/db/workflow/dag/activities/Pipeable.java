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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.CheckpointOutputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.CheckpointWriter;

// TODO: write test to ensure at most 1 output was specified
public interface Pipeable extends Activity {

    /**
     * Whether this activity instance can be piped, given the input tuple types and setting values.
     * If this method is overridden, it is required to also provide a custom execute implementation.
     * This is necessary in the case that canPipe returns false.
     *
     * @param inTypes preview of the input types
     * @param settings preview of the settings
     * @return false if this activity instance cannot be piped for certain, true otherwise
     */
    default boolean canPipe( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) {
        return true;
    }

    /**
     * Default implementation that uses the {@code pipe()} method for executing this activity without a PipeExecutor.
     * It is always desirable to override this method with a custom implementation that does not depend on {@code pipe()}.
     * An issue of this implementation is that it does not check the ExecutionContext for interrupts -> no early return in case of an interrupt!
     *
     * @param inputs a list of input readers for each input specified by the annotation.
     * @param settings the instantiated setting values, according to the specified settings annotations
     * @param ctx ExecutionContext to be used for creating checkpoints, updating progress and periodically checking for an abort
     * @throws Exception in case the execution fails or is interrupted at any point
     */
    @Override
    default void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx ) throws Exception {
        List<AlgDataType> inputTypes = inputs.stream().map( CheckpointReader::getTupleType ).toList();
        assert canPipe(
                inputTypes.stream().map( Optional::of ).toList(),
                settings.entrySet().stream()
                        .collect( Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> Optional.ofNullable( entry.getValue() )
                        ) ) ) : "Cannot use the default execute implementation of Pipeable if canPipe returns false.";

        AlgDataType type = lockOutputType( inputTypes, settings );
        List<InputPipe> inPipes = inputs.stream().map( reader -> (InputPipe) new CheckpointInputPipe( reader ) ).toList();
        PipeExecutionContext pipeCtx = (ExecutionContextImpl) ctx;
        try ( CheckpointWriter writer = ctx.createWriter( 0, type, true ) ) {
            OutputPipe outPipe = new CheckpointOutputPipe( type, writer );
            pipe( inPipes, outPipe, settings, pipeCtx );
        }
    }

    /**
     * Define the output type of this pipe.
     * Afterward, it may no longer be changed until reset() is called.
     *
     * @param inTypes the types of the input pipes
     * @param settings the resolved settings
     * @return the compulsory output type of this instance until the next call to reset().
     */
    AlgDataType lockOutputType( List<AlgDataType> inTypes, Map<String, SettingValue> settings );

    /**
     * Successively consumes the tuples of the input pipe(s) and forwards produced tuples to the output pipe.
     * There does not have to be a 1:1 relationship between input and output tuples.
     * The Pipeable activity is not expected to close the output themselves. This is done by the executor
     * after the pipe method returns.
     *
     * @param inputs the InputPipes to iterate over
     * @param output the output pipe for sending output tuples to that respect the locked output type
     * @param settings the resolved settings
     * @param ctx ExecutionContext to be used for updating progress (interrupt checking is done automatically by the pipes)
     * @throws PipeInterruptedException if thread gets interrupted during execution (used for prematurely stopping execution)
     * @throws Exception if some other problem occurs during execution that requires the execution of the pipe to stop
     */
    void pipe( List<InputPipe> inputs, OutputPipe output, Map<String, SettingValue> settings, PipeExecutionContext ctx ) throws Exception;


    class PipeInterruptedException extends RuntimeException {

        // Constructor that accepts a cause
        public PipeInterruptedException( Throwable cause ) {
            super( "The pipe operation was interrupted.", cause );
        }

    }

}
