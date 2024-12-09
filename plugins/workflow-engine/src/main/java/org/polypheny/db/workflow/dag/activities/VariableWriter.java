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
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

public interface VariableWriter extends Activity {

    /**
     * Whether the activity wants to be able to write variables during execution.
     * Activities that implement this interface are typically pure writers, the default return value is thus {@code Optional.of(true)}.
     * <p>
     * We let activities decide dynamically whether they intend to write variables to allow any activity to write variables.
     * The goal of restricting variable writing is to minimize the impact on performance (writing variables disables fusion and pipelining).
     * <p>
     * Returning a non-empty Optional is definitive and the decision may not be changed at a later point.
     * If no decision can be made, an empty Optional is returned instead
     *
     * @return an Optional containing the final decision whether this activity intends to write variables, or an empty Optional if it cannot be stated at this point
     */
    default Optional<Boolean> requestsToWrite( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) {
        return Optional.of( true );
    }


    /**
     * The slightly different execute method that comes with a writable variable store.
     * In the case that this method is called, it can be assumed that {@code updateVariables} was NOT called.
     */
    void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer );

}
