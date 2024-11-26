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
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;

// TODO: write test to ensure at most 1 output was specified
public interface Pipeable extends Activity {

    default boolean canPipe( List<Optional<AlgDataType>> inType, Map<String, Optional<SettingValue>> settings ) {
        return true;
    }

    @Override
    default void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx ) throws Exception {
        // TODO: add default implementation that calls pipe().
        throw new NotImplementedException();
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
