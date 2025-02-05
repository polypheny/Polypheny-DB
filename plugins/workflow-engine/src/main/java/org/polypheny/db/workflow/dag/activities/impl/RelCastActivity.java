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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.CastSetting;
import org.polypheny.db.workflow.dag.settings.CastValue;
import org.polypheny.db.workflow.dag.settings.CastValue.SingleCast;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relCast", displayName = "Change Column Types", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL, description = "A table with the same columns as the input table, but with possibly changed column types.") },
        shortDescription = "Cast the values of specific columns of the input table to a different type. In case the cast is not possible, the activity fails."
)

@CastSetting(key = "cast", displayName = "Casts", defaultType = PolyType.BIGINT,
        shortDescription = "Specify the columns to cast. Any unspecified column remains unaltered.")

@SuppressWarnings("unused")
public class RelCastActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        CastValue cast = settings.get( "cast", CastValue.class ).orElse( null );
        if ( type != null && cast != null ) {
            return RelType.of( getType( type, cast ) ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType( inTypes.get( 0 ), settings.get( "cast", CastValue.class ) );
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return Optional.of( false );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        // TODO: implement or remove pipe
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        CastValue cast = settings.get( "cast", CastValue.class );
        AlgDataType type = getType( inputs.get( 0 ).getTupleType(), cast );
        RexBuilder builder = cluster.getRexBuilder();

        int i = 0;
        List<RexNode> casts = new ArrayList<>();
        for ( AlgDataTypeField field : type.getFields() ) {
            casts.add( builder.makeCast( field.getType(), builder.makeInputRef( inputs.get( 0 ), i ) ) );
            i++;
        }
        ctx.logInfo( "Casts: " + casts );
        return LogicalRelProject.create( inputs.get( 0 ), casts, type );
    }


    private AlgDataType getType( AlgDataType inType, CastValue cast ) {
        Builder builder = ActivityUtils.getBuilder();
        Map<String, SingleCast> casts = cast.asMap();
        for ( AlgDataTypeField field : inType.getFields() ) {
            // TODO: check compatibility?
            if ( casts.containsKey( field.getName() ) ) {
                builder.add( field.getName(), null, casts.get( field.getName() ).getAlgDataType() );
            } else {
                builder.add( field );
            }
        }
        return builder.build();
    }

}
