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

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FilterSetting;
import org.polypheny.db.workflow.dag.settings.FilterValue;
import org.polypheny.db.workflow.dag.settings.FilterValue.Operator;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "relEfficientFilter", displayName = "Filter Rows", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.REL, description = "The input table.") },
        outPorts = { @OutPort(type = PortType.REL, description = "A table containing all matching rows from the input table.") },
        shortDescription = "Filters the rows of a table based on a list of filter conditions."
)

@FilterSetting(key = "filter", displayName = "Filter Conditions", pos = 1,
        excludedOperators = { Operator.HAS_KEY, Operator.IS_OBJECT },
        modes = { SelectMode.EXACT, SelectMode.REGEX, SelectMode.INDEX })
@BoolSetting(key = "negate", displayName = "Negate Filter", pos = 2, defaultValue = false, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "If enabled, the filter is negated.")

@SuppressWarnings("unused")
public class RelEfficientFilterActivity implements Activity, Pipeable, Fusable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof RelType type ) {
            if ( settings.keysPresent( "filter" ) ) {
                FilterValue filter = settings.getOrThrow( "filter", FilterValue.class );
                if ( filter.getConditions().isEmpty() ) {
                    throw new InvalidSettingException( "At least one condition must be specified", "filter" );
                }
                List<String> missing = filter.getMissingRequiredFields( type.getNullableType().getFieldNames() );
                if ( !missing.isEmpty() ) {
                    throw new InvalidSettingException( "Missing columns: " + missing, "filter" );
                }
            }
        }
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        if ( settings.get( "filter", FilterValue.class ).canBeFused() ) {
            Fusable.super.execute( inputs, settings, ctx );
        } else {
            Pipeable.super.execute( inputs, settings, ctx );
        }
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "filter", FilterValue.class ).map( FilterValue::canBeFused );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        FilterValue filter = settings.get( "filter", FilterValue.class );
        RexNode rexNode = filter.getRexNode( inputs.get( 0 ).getTupleType(), cluster.getRexBuilder() );
        if ( rexNode == null ) {
            return inputs.get( 0 ); // no filtering
        }
        if ( settings.getBool( "negate" ) ) {
            rexNode = cluster.getRexBuilder().makeCall( OperatorRegistry.get( OperatorName.NOT ), rexNode );
        }
        return LogicalRelFilter.create( inputs.get( 0 ), rexNode );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        InputPipe input = inputs.get( 0 );

        FilterValue filter = settings.get( "filter", FilterValue.class );
        Predicate<List<PolyValue>> predicate = filter.getRelPredicate( input.getType().getFieldNames() );
        if ( settings.getBool( "negate" ) ) {
            predicate = predicate.negate();
        }

        for ( List<PolyValue> row : input ) {
            if ( predicate.test( row ) ) {
                if ( !output.put( row ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }

}
