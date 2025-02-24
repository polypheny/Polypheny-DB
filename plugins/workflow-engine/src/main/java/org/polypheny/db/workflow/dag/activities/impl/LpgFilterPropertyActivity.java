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

package org.polypheny.db.workflow.dag.activities.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.FilterSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.FilterValue;
import org.polypheny.db.workflow.dag.settings.FilterValue.Operator;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

@ActivityDefinition(type = "lpgPropertyFilter", displayName = "Filter Graph by Properties", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph.") },
        outPorts = { @OutPort(type = PortType.LPG, description = "A graph containing all matching nodes and edges from the input graph.") },
        shortDescription = "Computes a subgraph of the input that only includes nodes and edges that meet the specified filter criteria."
)
@FieldSelectSetting(key = "requiredFields", displayName = "Required Properties", simplified = true, targetInput = -1, pos = 0,
        shortDescription = "Specify all properties that must exist in any target. Useful in combination with a condition, as conditions evaluate to true if the property does not exist.")
@FilterSetting(key = "filter", displayName = "Conditions", pos = 1,
        modes = { SelectMode.EXACT, SelectMode.REGEX }, targetInput = -1,
        excludedOperators = { Operator.IS_OBJECT },
        shortDescription = "Define a list of conditions on properties. If a property does not exist, the condition evaluates to true.")

@FieldSelectSetting(key = "labels", displayName = "Targets", simplified = true, targetInput = 0, pos = 0, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "Specify the target nodes or edges by their label(s). If no label is specified, all become targets.")
@BoolSetting(key = "fail", displayName = "Fail on Rejection", pos = 1, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "If enabled, a rejected node or edge results in the activity to fail.")
@BoolSetting(key = "nodes", displayName = "Filter Nodes", defaultValue = true, pos = 2, group = GroupDef.ADVANCED_GROUP)
@BoolSetting(key = "edges", displayName = "Filter Edges", defaultValue = true, pos = 3, group = GroupDef.ADVANCED_GROUP)
@BoolSetting(key = "negate", displayName = "Negate Filter", pos = 4, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "If enabled, the filter is negated. Non-target nodes or edges are not affected by this.")

@SuppressWarnings("unused")
public class LpgFilterPropertyActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof LpgType lpgType ) {
            return lpgType.asOutTypes();
        }
        return LpgType.of().asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean fail = settings.getBool( "fail" );
        boolean isNodes = settings.getBool( "nodes" );
        boolean isEdges = settings.getBool( "edges" );
        FilterValue filter = settings.get( "filter", FilterValue.class );
        Predicate<PolyDictionary> predicate = filter.getLpgPredicate();

        List<PolyString> required = settings.get( "requiredFields", FieldSelectValue.class ).getInclude().stream().map( PolyString::of ).toList();
        if ( !required.isEmpty() ) {
            Predicate<PolyDictionary> basePredicate = predicate;
            predicate = d -> {
                if ( required.stream().allMatch( d::containsKey ) ) {
                    return basePredicate.test( d );
                }
                return false;
            };
        }
        if ( settings.getBool( "negate" ) ) {
            predicate = predicate.negate();
        }

        Set<PolyString> nodeIds = new HashSet<>();
        for ( PolyNode node : input.getNodeIterable() ) {
            if ( isNodes && ActivityUtils.matchesLabelList( node, labels ) ) {
                if ( !predicate.test( node.properties ) ) {
                    if ( fail ) {
                        throw new GenericRuntimeException( "Detected node that does not match the filter criteria: " + node );
                    }
                    continue;
                }
            }
            nodeIds.add( node.id );
            if ( !output.put( node ) ) {
                finish( inputs );
                return;
            }
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( !nodeIds.contains( edge.left ) || !nodeIds.contains( edge.right ) ) {
                continue; // node was removed
            }
            if ( isEdges && ActivityUtils.matchesLabelList( edge, labels ) ) {
                if ( !predicate.test( edge.properties ) ) {
                    if ( fail ) {
                        throw new GenericRuntimeException( "Detected edge that does not match the filter criteria: " + edge );
                    }
                    continue;
                }
            }

            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }

}
