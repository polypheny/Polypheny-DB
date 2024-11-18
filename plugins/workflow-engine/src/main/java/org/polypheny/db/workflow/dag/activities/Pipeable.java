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

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.queue.InputPipe;
import org.polypheny.db.workflow.engine.execution.queue.OutputPipe;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;

// TODO: write test to ensure at most 1 input and output was specified
public interface Pipeable extends Activity {

    default boolean canPipe( AlgDataType inType, Map<String, Optional<SettingValue>> settings ) {
        return true;
    }

    @Override
    default void execute( CheckpointReader[] inputs, ExecutionContext ctx ) throws Exception {
        // TODO: add default implementation that calls pipe().
        throw new NotImplementedException();
    }

    /**
     * Define the output type of this pipe.
     * Afterward, it may no longer be changed until reset() is called.
     *
     * @param inType the type of the input pipe
     * @param settings the resolved settings
     * @return the compulsory output type of this instance until the next call to reset().
     */
    AlgDataType lockOutputType( AlgDataType inType, Map<String, SettingValue> settings );

    void pipe( InputPipe input, OutputPipe output, ExecutionContext ctx );


}
