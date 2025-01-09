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
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.Group;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;

@ActivityDefinition(type = "identity", displayName = "Identity", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "Input table") },
        outPorts = { @OutPort(type = PortType.REL, description = "Output table, equal to the input table.") },
        shortDescription = "The identity activity outputs the input table unchanged.",
        longDescription = """
                This is the long description of the identity activity. It is possible to use *markdown*!
                # Title
                ## Subtitle
                ### Subsubtitle
                
                Now a list:
                - first
                - second
                - third
                
                code: `inline code`
                or block:
                
                ```python
                def my_function():
                    print('test')
                ```
                
                > This is a blockquote
                """)

@IntSetting(key = "I1", displayName = "FIRST", defaultValue = 2, shortDescription = "This setting doesn't do anything.")
@StringSetting(key = "S1", displayName = "SECOND", shortDescription = "This setting doesn't do anything.")

@Group(key = "groupA", displayName = "Group A",
        subgroups = { @Subgroup(key = "a", displayName = "Sub1") }
)
@IntSetting(key = "I2", displayName = "THIRD", defaultValue = 0, isList = true, group = "groupA", shortDescription = "This setting doesn't do anything.")
@StringSetting(key = "S2", displayName = "FOURTH", defaultValue = "test", isList = true, group = "groupA", subGroup = "a", shortDescription = "This setting doesn't do anything.")

@SuppressWarnings("unused")
public class IdentityActivity implements Activity, Fusable, Pipeable {


    public IdentityActivity() {
    }


    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) throws ActivityException {
        List<Optional<AlgDataType>> outTypes = new ArrayList<>(); // TODO: this is a workaround since inType could be null and List.of() requires non null
        outTypes.add( inTypes.get( 0 ) );
        return outTypes;
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        RelReader input = (RelReader) inputs.get( 0 );
        ctx.createRelWriter( 0, input.getTupleType(), false )
                .write( input.getIterator() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            output.put( value );
        }
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        // to make it more interesting, we add a project activity that doesn't change the tupleType
        return LogicalRelProject.identity( inputs.get( 0 ) );
    }


    @Override
    public void reset() {
    }

}
