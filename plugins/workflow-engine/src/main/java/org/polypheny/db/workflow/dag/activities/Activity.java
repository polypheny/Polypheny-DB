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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

public interface Activity {

    /**
     * This method computes the output tuple-types by considering (a preview of) input types and settings.
     * If the input types or settings are not available or cannot be validated yet, the output type is set to an empty {@link Optional}
     * for the outputs that depend on it. The same holds for outputs whose type can only be determined during execution.
     * If any available setting or input type results in a contradiction or invalid state,
     * an {@link ActivityException} is thrown.
     * If a setting, input or output type is available, it is guaranteed to not change anymore.
     *
     * @param inTypes a list of {@link Optional<AlgDataType>} representing the input tuple types.
     * @param settings a map of setting keys to {@link Optional<SettingValue>} representing the available settings, i.e. all settings that do not contain variables.
     * @return a list of {@link Optional<AlgDataType>} representing the expected output tuple types.
     * If an output type cannot be determined at this point, the corresponding {@link Optional} will be empty.
     * @throws ActivityException if any available setting or input type results in a contradiction or invalid state.
     */
    List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException;

    static List<Optional<AlgDataType>> wrapType( @Nullable AlgDataType type ) {
        return List.of( Optional.ofNullable( type ) );
    }


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

    default DataStateMerger getDataStateMerger() {
        // typically depends on the activity type
        return DataStateMerger.AND;
    }


    default ControlStateMerger overrideControlStateMerger() {
        return null; // typically depends on the activity config -> we return null
    }

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


    enum DataStateMerger {
        AND( DataStateMerger::andMerger ),
        OR( DataStateMerger::orMerger );

        private final Function<List<EdgeState>, Boolean> merger;


        DataStateMerger( Function<List<EdgeState>, Boolean> merger ) {
            this.merger = merger;
        }


        /**
         * Computes whether an activity is NOT aborted based on its data edge states.
         *
         * @param dataEdges the EdgeState of all data inputs of an activity
         * @return false if the data edge states result in an abort, true otherwise.
         */
        public boolean merge( List<EdgeState> dataEdges ) {
            return merger.apply( dataEdges );
        }


        private static boolean andMerger( List<EdgeState> dataEdges ) {
            // abort if any dataEdge is inactive
            return !dataEdges.contains( EdgeState.INACTIVE );
        }


        private static boolean orMerger( List<EdgeState> dataEdges ) {
            // only abort if all dataEdges are inactive. Useful for merging activities
            if ( dataEdges.isEmpty() ) {
                return true;
            }
            return !dataEdges.stream().allMatch( state -> state == EdgeState.INACTIVE );
        }
    }


    enum ControlStateMerger {
        /**
         * Corresponds to ANDing all success control edges and ORing all fail control edges
         * The merged result is therefore only ACTIVE if all success control edges are active and at least
         * one of the fail control edges (if present) is active.
         */
        AND_OR( ControlStateMerger::andOrMerger ),

        /**
         * Corresponds to ANDing all control edges.
         * The merged result is only ACTIVE, if all control edges are ACTIVE.
         */
        AND_AND( ControlStateMerger::andAndMerger );

        private final BiFunction<List<EdgeState>, List<EdgeState>, EdgeState> merger;


        ControlStateMerger( BiFunction<List<EdgeState>, List<EdgeState>, EdgeState> merger ) {
            this.merger = merger;
        }


        /**
         * Merges the states of all incoming control edges of an activity into a single state.
         * This state can be interpreted as:
         * <ul>
         *     <li>{@link EdgeState#INACTIVE}: trigger an abort</li>
         *     <li>{@link EdgeState#IDLE}: wait for more control edges to become active or inactive</li>
         *     <li>{@link EdgeState#ACTIVE}: the activity is ready to be executed</li>
         * </ul>
         *
         * @param successEdges the EdgeStates of all onSuccess control inputs of the activity
         * @param failEdges the EdgeStates of all onFail control inputs of the activity
         * @return the EdgeState resulting from the merge of all input edges
         */
        public EdgeState merge( List<EdgeState> successEdges, List<EdgeState> failEdges ) {
            return merger.apply( successEdges, failEdges );
        }


        private static EdgeState andOrMerger( List<EdgeState> successEdges, List<EdgeState> failEdges ) {
            // ANDing all successEdges and ORing all failEdges

            if ( successEdges.contains( EdgeState.INACTIVE ) ) {
                return EdgeState.INACTIVE;
            }

            if ( !failEdges.isEmpty() ) {
                if ( failEdges.stream().allMatch( state -> state == EdgeState.INACTIVE ) ) {
                    return EdgeState.INACTIVE;
                }

                if ( !failEdges.contains( EdgeState.ACTIVE ) ) {
                    return EdgeState.IDLE;
                }
            }

            if ( successEdges.contains( EdgeState.IDLE ) ) {
                return EdgeState.IDLE;
            }
            return EdgeState.ACTIVE;
        }


        private static EdgeState andAndMerger( List<EdgeState> successEdges, List<EdgeState> failEdges ) {
            List<EdgeState> allEdges = new ArrayList<>();
            allEdges.addAll( successEdges );
            allEdges.addAll( failEdges );

            if ( allEdges.contains( EdgeState.INACTIVE ) ) {
                return EdgeState.INACTIVE;
            }
            if ( allEdges.contains( EdgeState.IDLE ) ) {
                return EdgeState.IDLE;
            }
            return EdgeState.ACTIVE;
        }

    }

}
