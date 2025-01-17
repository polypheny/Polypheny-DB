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
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.ListValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "fieldNameToVar", displayName = "Extract Field Names", categories = { ActivityCategory.VARIABLES, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) }, // TODO: add support for docs
        outPorts = {}
)
@StringSetting(key = "variableName", displayName = "Variable Name", defaultValue = "field_names", minLength = 1, maxLength = 128)
@IntSetting(key = "fields", displayName = "Target Fields", isList = true, defaultValue = 0, min = 0)

@SuppressWarnings("unused")
public class FieldNameToVariableActivity implements VariableWriter {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) {
        String variableName = settings.get( "variableName", StringValue.class ).getValue();
        List<Integer> targetFields = settings.get( "fields", ListValue.class ).getValues();

        List<String> fieldNames = inputs.get( 0 ).getTupleType().getFieldNames();
        List<String> values;
        if ( targetFields.isEmpty() ) {
            values = fieldNames;
        } else {
            values = new ArrayList<>();
            for ( int i : targetFields ) {
                values.add( fieldNames.get( i ) );
            }
        }
        writer.setVariable( variableName, values );
    }

}
