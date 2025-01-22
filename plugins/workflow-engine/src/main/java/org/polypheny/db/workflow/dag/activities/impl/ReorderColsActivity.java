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

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "reorderCols", displayName = "Select / Reorder Columns", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "The input table") },
        outPorts = { @OutPort(type = PortType.REL, description = "A Table containing the selected subset of columns from the input table in the specified order.") },
        shortDescription = "Select and reorder the columns of the input table."
)
@FieldSelectSetting(key = "cols", displayName = "Columns", reorder = true, defaultAll = true,
        shortDescription = "Specify the names of the columns to include. Alternatively, you can include all columns except for the excluded ones. The \"" + StorageManager.PK_COL + "\" column must always be included.")

@SuppressWarnings("unused")
public class ReorderColsActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Optional<FieldSelectValue> optionalCols = settings.get( "cols", FieldSelectValue.class );

        if ( inTypes.get( 0 ).isEmpty() || optionalCols.isEmpty() ) {
            return UnknownType.ofRel().asOutTypes();
        }

        return TypePreview.ofType( getOutType( inTypes.get( 0 ).getNullableType(), optionalCols.get() ) ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        AlgDataType type = getOutType( inputs.get( 0 ).getTupleType(), settings.get( "cols", FieldSelectValue.class ) );
        List<Integer> inCols = type.getFieldNames().stream().map(
                name -> inputs.get( 0 ).getTupleType().getFieldNames().indexOf( name )
        ).toList();

        List<RexIndexRef> refs = IntStream.range( 0, type.getFieldCount() ).mapToObj( i -> new RexIndexRef( inCols.get( i ), type.getFields().get( i ).getType() ) ).toList();
        return LogicalRelProject.create( inputs.get( 0 ), refs, type );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getOutType( inTypes.get( 0 ), settings.get( "cols", FieldSelectValue.class ) );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        List<Integer> inCols = output.getType().getFieldNames().stream().map(
                name -> inputs.get( 0 ).getType().getFieldNames().indexOf( name )
        ).toList();

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            output.put( inCols.stream().map( row::get ).toList() );
        }
    }


    private AlgDataType getOutType( AlgDataType type, @NotNull FieldSelectValue cols ) throws ActivityException {
        List<String> selected = cols.getSelected( type.getFieldNames() );
        return ActivityUtils.filterFields( type, selected, true );
    }

}
