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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.AggregateSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.AggregateValue;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;

@ActivityDefinition(type = "relAggregate", displayName = "Aggregate Rows", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "The input table.") },
        outPorts = { @OutPort(type = PortType.REL, description = "The output table with aggregated values.") },
        shortDescription = "Aggregates row values by grouping them according to specified columns."
)

@FieldSelectSetting(key = "group", displayName = "Group By Columns", pos = 0,
        simplified = true, reorder = true,
        shortDescription = "The columns to group by.")
@AggregateSetting(key = "aggregates", displayName = "Aggregated Values", pos = 1,
        allowedFunctions = { "COUNT", "SUM", "SUM0", "AVG", "MIN", "MAX" },
        shortDescription = "The columns whose values are aggregated by the specified function. An optional alias name for the resulting column can be provided.")
@SuppressWarnings("unused")
public class RelAggregateActivity implements Activity, Fusable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType inType = inTypes.get( 0 ).getNullableType();
        if ( inType == null || !settings.allPresent() ) {
            return UnknownType.ofRel().asOutTypes();
        }
        List<String> group = settings.getOrThrow( "group", FieldSelectValue.class ).getInclude();
        AggregateValue agg = settings.getOrThrow( "aggregates", AggregateValue.class );
        try {
            agg.validate( inType, group );
        } catch ( IllegalArgumentException e ) {
            throw new InvalidSettingException( e.getMessage(), "aggregates" );
        }
        return RelType.of( getType( inType, group, agg ) ).asOutTypes();
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgDataType inType = inputs.get( 0 ).getTupleType();
        List<String> cols = inType.getFieldNames();
        List<String> group = settings.get( "group", FieldSelectValue.class ).getInclude();
        AggregateValue agg = settings.get( "aggregates", AggregateValue.class );

        AlgNode aggNode = LogicalRelAggregate.create(
                inputs.get( 0 ),
                ImmutableBitSet.of( group.stream().map( cols::indexOf ).toList() ),
                null,
                agg.toAggregateCalls( inputs.get( 0 ), group ) );
        return ActivityUtils.addPkCol( aggNode, cluster );
    }


    private AlgDataType getType( AlgDataType inType, List<String> group, AggregateValue agg ) {
        Builder builder = ActivityUtils.getBuilder()
                .add( StorageManager.PK_COL, null, PolyType.BIGINT );
        for ( String g : group ) {
            builder.add( inType.getField( g, true, false ) );
        }
        agg.addAggregateColumns( builder, inType, factory, group );
        AlgDataType outType = builder.uniquify().build();
        ActivityUtils.validateFieldNames( outType.getFieldNames() );
        return outType;
    }

}
