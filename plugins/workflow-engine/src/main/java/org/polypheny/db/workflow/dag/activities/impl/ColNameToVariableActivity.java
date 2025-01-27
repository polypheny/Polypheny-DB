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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "colNameToVar", displayName = "Column Names to Variable", categories = { ActivityCategory.VARIABLES, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = {},
        shortDescription = "Create a variable that contains the column names of the input table."
)
@StringSetting(key = "variableName", displayName = "Variable Name", defaultValue = "columnNames", minLength = 1, maxLength = 128, longDescription = "The name of the variable to be generated.")
@BoolSetting(key = "specifyColumn", displayName = "Specify Column", defaultValue = false, shortDescription = "If true, only the specified target column is written to the variable.")
@IntSetting(key = "target", displayName = "Target Column Index", defaultValue = 0, min = 0, subPointer = "specifyColumn", subValues = { "true" })

@SuppressWarnings("unused")
public class ColNameToVariableActivity implements VariableWriter {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        if ( type != null && settings.keysPresent( "specifyColumn", "target" ) ) {
            if ( settings.getOrThrow( "specifyColumn", BoolValue.class ).getValue() ) {
                int target = settings.getOrThrow( "target", IntValue.class ).getValue();
                if ( type.getFieldCount() <= target ) {
                    throw new InvalidSettingException( "The column with the specified index does not exist.", "target" );
                }
            }
        }
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) {
        String variableName = settings.get( "variableName", StringValue.class ).getValue();
        boolean specifyColumn = settings.get( "specifyColumn", BoolValue.class ).getValue();
        int target = settings.get( "target", IntValue.class ).getValue();

        List<String> fieldNames = inputs.get( 0 ).getTupleType().getFieldNames();
        if ( specifyColumn ) {
            writer.setVariable( variableName, fieldNames.get( target ) );
        } else {
            writer.setVariable( variableName, fieldNames );
        }
    }

}
