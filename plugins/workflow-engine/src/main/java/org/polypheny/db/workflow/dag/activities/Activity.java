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
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;

public interface Activity {

    /**
     * This method computes the output tuple-types by considering (a preview of) input types and settings.
     * If the input types or settings are not available or cannot be validated yet, the output type is set to an empty {@link Optional}
     * for the outputs that depend on it. The same holds for outputs whose type can only be determined during execution.
     * If any available setting or input type results in a contradiction or invalid state,
     * an {@link ActivityException} is thrown.
     *
     * @param inTypes a list of {@link Optional<AlgDataType>} representing the input tuple types.
     * @param settings a map of setting keys to {@link Optional<SettingValue>} representing the available settings.
     * @return a list of {@link Optional<AlgDataType>} representing the expected output tuple types.
     * If an output type cannot be determined at this point, the corresponding {@link Optional} will be empty.
     * @throws ActivityException if any available setting or input type results in a contradiction or invalid state.
     */
    List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException;


    /**
     * This method is called just before execution starts and can be used to write variables based on input tuple types and settings.
     * To be able to update variables while having access to the input data, the activity should instead implement {@link VariableWriter}.
     *
     * @param inTypes a list of {@link AlgDataType} representing the input tuple types.
     * @param settings a map of setting keys to {@link SettingValue} representing the settings.
     * @param variables a WritableVariableStore to be used for updating any variable values.
     */
    default void updateVariables( List<AlgDataType> inTypes, Map<String, SettingValue> settings, WritableVariableStore variables ) {
    }

    // settings do NOT include values from the updateVariables step.

    /**
     * Execute this activity.
     * Any input CheckpointReaders are provided and expected to be closed by the caller.
     * CheckpointWriters for any outputs are created from the ExecutionContext.
     * The settings do not incorporate any changes to variables from {@code  updateVariables()}.
     *
     * @param inputs a list of input readers for each input specified by the annotation.
     * @param settings the instantiated setting values, according to the specified settings annotations
     * @param ctx ExecutionContext to be used for creating checkpoints, updating progress and periodically checking for an abort
     * @throws Exception in case the execution fails or is interrupted at any point
     */
    void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx ) throws Exception; // default execution method

    /**
     * Reset any execution-specific state of this activity.
     * It is guaranteed to be called before execution starts.
     */
    void reset();

    enum PortType {
        ANY,
        REL,
        DOC,
        LPG;


        public boolean canReadFrom( PortType other ) {
            return this == other || this == ANY;
        }


        public boolean canWriteTo( PortType other ) {
            return this == other || other == ANY;
        }


        /**
         * Returns the corresponding DataModel enum.
         * In case of ANY, DataModel.RELATIONAL is returned.
         *
         * @return the corresponding DataModel
         */
        public DataModel getDataModel() {
            return switch ( this ) {
                case ANY, REL -> DataModel.RELATIONAL;
                case DOC -> DataModel.DOCUMENT;
                case LPG -> DataModel.GRAPH;
            };
        }
    }


    enum ActivityCategory {
        EXTRACT,
        TRANSFORM,
        LOAD
        // more granular categories are also thinkable
    }

}
