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
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
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
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.FieldRenameSetting;
import org.polypheny.db.workflow.dag.settings.FieldRenameValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "renameCols", displayName = "Rename Columns", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "The input table") },
        outPorts = { @OutPort(type = PortType.REL, description = "A table with the same columns as the input table, but with possibly different names") },
        shortDescription = "Rename the columns of a table by defining rules."
)
@FieldRenameSetting(key = "rename", displayName = "Renaming Rules", allowRegex = true, allowIndex = true,
        shortDescription = "The source columns can be selected by their actual (constant) name, their index or with Regex. "
                + "The replacement can always reference capture groups such as '$0' for the original name.",
        longDescription = """
                The source columns can be selected by their actual (constant) name, their index or by using a regular expression.
                In the latter case it is possible to specify capturing groups.
                
                In any mode, the replacement can reference a capture group ('$0', '$1'...). For instance, the replacement 'abc$0 adds the prefix 'abc' to each matching column.
                
                Regular expressions are given in the [Java Regex dialect](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
                """)

@SuppressWarnings("unused")
public class RelRenameActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        FieldRenameValue rename = settings.get( "rename", FieldRenameValue.class ).orElse( null );

        if ( type != null && rename != null ) {
            return RelType.of( getOutType( type, rename ) ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgDataType type = getOutType( inputs.get( 0 ).getTupleType(), settings.get( "rename", FieldRenameValue.class ) );
        ctx.logInfo( "Renaming fields to " + type.getFieldNames() );

        List<RexIndexRef> refs = IntStream.range( 0, type.getFieldCount() ).mapToObj( i -> new RexIndexRef( i, type.getFields().get( i ).getType() ) ).toList();
        return LogicalRelProject.create( inputs.get( 0 ), refs, type );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getOutType( inTypes.get( 0 ), settings.get( "rename", FieldRenameValue.class ) );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        ctx.logInfo( "Renaming fields to " + output.getType().getFieldNames() );
        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            if ( !output.put( row ) ) {
                inputs.forEach( InputPipe::finishIteration );
                return;
            }
        }
    }


    private AlgDataType getOutType( AlgDataType inputType, FieldRenameValue rename ) throws InvalidSettingException {
        List<String> inNames = inputType.getFieldNames();
        Map<String, String> mapping = rename.getMapping( inNames );
        AlgDataType outType = ActivityUtils.renameFields( inputType, mapping );
        Optional<String> invalid = ActivityUtils.findInvalidFieldName( outType.getFieldNames() );
        if (invalid.isPresent()) {
            throw new InvalidSettingException( "Invalid column name: " + invalid.get(), "rename" );
        }
        return outType;
    }

}
