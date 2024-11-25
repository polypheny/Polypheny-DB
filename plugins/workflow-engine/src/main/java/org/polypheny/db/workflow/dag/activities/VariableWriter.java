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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;

public interface VariableWriter extends Activity {

    /**
     * Whether the activity wants to be able to write variables during execution.
     * Activities that implement this interface are typically pure writers, the default return value is thus {@code true}.
     *
     * We let activities decide dynamically whether they intend to write variables to allow any activity to write variables.
     * The goal of restricting variable writing is to minimize the impact on performance (writing variables disables fusion and pipelining).
     *
     * Returning false is definitive. Returning true at a later point during the same execution might no longer be respected.
     *
     * @return true if this activity intends to write variables during execution.
     */
    default boolean requestsToWrite( Optional<AlgDataType>[] inTypes, Map<String, Optional<SettingValue>> settings ) {
        return true;
    }


    /**
     * The slightly different execute method that comes with a writable variable store.
     * In the case that this method is called, it can be assumed that {@code updateVariables} was NOT called.
     */
    void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx, WritableVariableStore writer );

}
