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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.AggregateSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.AggregateValue;
import org.polypheny.db.workflow.dag.settings.AggregateValue.AggregateEntry;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;

@ActivityDefinition(type = "docAggregate", displayName = "Aggregate Documents", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection of documents.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The output collection with aggregated values.") },
        shortDescription = "Aggregates document values by grouping them according to a specified field."
)

@StringSetting(key = "group", displayName = "Group By Field", pos = 0,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, nonBlank = true,
        shortDescription = "The field to group by.")
@AggregateSetting(key = "aggregates", displayName = "Aggregated Values", pos = 1,
        shortDescription = "The fields whose values are aggregated by the specified function. An optional alias for the resulting field can be provided.")
@SuppressWarnings("unused")
public class DocAggregateActivity implements Activity, Fusable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> fields = new HashSet<>();
        if ( settings.allPresent() ) {
            String group = settings.getString( "group" );
            AggregateValue agg = settings.getOrThrow( "aggregates", AggregateValue.class );
            if ( agg.getTargets().contains( group ) ) {
                throw new InvalidSettingException( "The group field cannot be aggregated at the same time", "aggregates" );
            }
            fields.add( deriveGroupName( group ) );
            agg.addUniquifiedAliases( fields );

            for ( AggregateEntry entry : agg.getAggregates() ) {
                if ( entry.getFunction().equals( "AVG" ) ) {
                    // TODO: Fix AVG aggregation (currently, the planner cannot implement the constructed tree)
                    throw new InvalidSettingException( "The AVG aggregate function is currently not supported.", "aggregates" );
                }
            }
        }
        return DocType.of( fields ).asOutTypes();
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        String group = settings.getString( "group" );
        String outGroup = deriveGroupName( group );
        AggregateValue agg = settings.get( "aggregates", AggregateValue.class );

        List<LaxAggregateCall> aggCalls = agg.toLaxAggregateCalls( 0, List.of( outGroup ) );
        AlgNode aggNode = LogicalDocumentAggregate.create(
                inputs.get( 0 ),
                ActivityUtils.getDocRexNameRef( group, 0 ),
                aggCalls );

        Map<String, RexNode> includes = new HashMap<>();
        includes.put( outGroup, ActivityUtils.getDocRexNameRef( docId.value, 0 ) ); // aggregation maps group to _id
        for ( LaxAggregateCall aggCall : aggCalls ) {
            includes.put( aggCall.name, ActivityUtils.getDocRexNameRef( aggCall.name, 0 ) );
        }
        return LogicalDocumentProject.create( aggNode, includes, List.of() );
    }


    private static String deriveGroupName( String group ) {
        return group.replace( ".", "_" );
    }

}
