/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "validateCount", displayName = "Validate Tuple Count", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.ANY) },
        outPorts = { @OutPort(type = PortType.ANY, description = "Identical to the input, but possibly with fewer tuples.") },
        shortDescription = "Validates the number of tuples (rows, documents or nodes) in the input. Both the minimum and maximum are specified as inclusive values."
)
@IntSetting(key = "min", displayName = "Minimum", pos = 0,
        min = 0, defaultValue = 0,
        shortDescription = "The minimum number of tuples in the input (inclusive).")
@IntSetting(key = "max", displayName = "Maximum", pos = 1,
        min = -1, defaultValue = -1,
        shortDescription = "The maximum number of tuples in the input (inclusive), or '-1' to disable.")
@BoolSetting(key = "limit", displayName = "Limit to Maximum", pos = 2,
        shortDescription = "If true, additional tuples than the specified maximum do not result in a failed validation, but are simply ignored.")
@SuppressWarnings("unused")
public class ValidateCountActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {

        if ( settings.keysPresent( "min", "max" ) ) {
            int min = settings.getInt( "min" );
            int max = settings.getInt( "max" );
            if ( max != -1 && max < min ) {
                throw new InvalidSettingException( "Maximum cannot be smaller than the minimum", "max" );
            }

            if ( settings.keysPresent( "limit" ) ) {
                if ( max == -1 && settings.getBool( "limit" ) ) {
                    throw new InvalidSettingException( "Cannot limit the result to maximum if no maximum is set.", "limit" );
                }
            }
        }

        if ( inTypes.get( 0 ).isPresent() ) {
            return inTypes.get( 0 ).asOutTypes();
        }
        return UnknownType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int min = settings.getInt( "min" );
        int max = settings.getInt( "max" );
        boolean limit = settings.getBool( "limit" );

        if ( ActivityUtils.getDataModel( inputs.get( 0 ).getType() ) == DataModel.GRAPH ) {
            lpgPipe( inputs.get( 0 ).asLpgInputPipe(), output, min, max, limit );
            return;
        }

        long count = 0;
        boolean insertValue = true;
        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            count++;
            if ( max >= 0 && count > max ) {
                if ( limit ) {
                    finish( inputs );
                    return;
                }
                throw new GenericRuntimeException( "Input contains more than " + max + " tuples." );
            }
            if ( insertValue ) {
                if ( !output.put( value ) ) {
                    insertValue = false;
                }
            }
        }
        if ( count < min ) {
            throw new GenericRuntimeException( "Input contains not enough tuples: " + count + " < " + min );
        }
    }


    private void lpgPipe( LpgInputPipe input, OutputPipe output, int min, int max, boolean limit ) throws InterruptedException {
        Set<PolyString> nodeIds = new HashSet<>();
        long count = 0;
        boolean insertValue = true;
        boolean ignoreRemaining = false;
        for ( PolyNode node : input.getNodeIterable() ) {
            if ( ignoreRemaining ) {
                continue; // iterate over skipped nodes
            }
            count++;
            if ( max >= 0 && count > max ) {
                if ( limit ) {
                    count = max;
                    ignoreRemaining = true;
                    continue;
                }
                throw new GenericRuntimeException( "Input contains more than " + max + " tuples." );
            }
            if ( insertValue ) {
                nodeIds.add( node.getId() );
                if ( !output.put( node ) ) {
                    insertValue = false;
                }
            }
        }
        if ( count < min ) {
            throw new GenericRuntimeException( "Input contains not enough tuples: " + count + " < " + min );
        }
        if ( !insertValue ) {
            return;
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( nodeIds.contains( edge.getLeft() ) && nodeIds.contains( edge.getRight() ) ) {
                if ( !output.put( edge ) ) {
                    input.finishIteration();
                    return;
                }
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        int min = settings.getInt( "min" );
        int max = settings.getInt( "max" );
        if ( max >= 0 ) {
            return max;
        }
        return min;
    }

}
