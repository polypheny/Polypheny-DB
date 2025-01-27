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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.Group;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "identity", displayName = "Identity", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.ANY, description = "Input table, collection or graph") },
        outPorts = { @OutPort(type = PortType.ANY, description = "Output, equal to the input.") },
        shortDescription = "The identity activity outputs the input unchanged. It is useful to observe the effect of having an activity with ANY output port type",
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
@StringSetting(key = "S1", displayName = "SECOND", shortDescription = "This setting doesn't do anything.", autoCompleteType = AutoCompleteType.FIELD_NAMES)
@IntSetting(key = "X1", displayName = "X1", shortDescription = "Depends on I1 being 42 or 420", subPointer = "I1", subValues = { "42", "420" })
@StringSetting(key = "X2", displayName = "X2", shortDescription = "Depends on X1 being 3", subPointer = "X1", subValues = { "3" })
@StringSetting(key = "X3", displayName = "X3", shortDescription = "Depends on I1/doesNotExist being 7", subPointer = "I1/doesNotExist", subValues = { "7" })

@Group(key = "groupA", displayName = "Group A",
        subgroups = { @Subgroup(key = "a", displayName = "Sub1") }
)
@IntSetting(key = "I2", displayName = "THIRD", defaultValue = 0, group = "groupA", shortDescription = "This setting doesn't do anything.")
@StringSetting(key = "S2", displayName = "FOURTH", defaultValue = "test", group = "groupA", subGroup = "a", shortDescription = "This setting doesn't do anything.")

@SuppressWarnings("unused")
public class IdentityActivity implements Activity, Fusable, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        CheckpointReader reader = inputs.get( 0 );
        switch ( reader.getDataModel() ) {
            case RELATIONAL -> {
                ctx.createRelWriter( 0, reader.getTupleType(), false )
                        .write( reader.getIterator() );
            }
            case DOCUMENT -> {
                ctx.createDocWriter( 0 )
                        .write( reader.getIterator() );
            }
            case GRAPH -> {
                ctx.createLpgWriter( 0 )
                        .write( reader.getIterator() );
            }
        }
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            if (!output.put( value )) {
                inputs.forEach( InputPipe::finishIteration );
                break;
            }
        }
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        return inputs.get( 0 );
    }

}
