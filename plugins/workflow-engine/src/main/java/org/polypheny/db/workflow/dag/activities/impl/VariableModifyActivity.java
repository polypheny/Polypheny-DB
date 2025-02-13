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

package org.polypheny.db.workflow.dag.activities.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map.Entry;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "varModify", displayName = "Modify Variable Structure", categories = { ActivityCategory.TRANSFORM, ActivityCategory.VARIABLES },
        inPorts = {},
        outPorts = {},
        shortDescription = "Modifies the structure of dynamic variables."
)

@EnumSetting(key = "mode", displayName = "Modification", pos = 0,
        options = { "move", "flatten", "remove", "arrWrap", "parse", "emptyObj", "emptyArr", "null" },
        displayOptions = { "Move / Rename", "Flatten", "Remove", "Wrap in Array", "Parse JSON", "Insert Empty Object", "Insert Empty Array", "Insert Null" },
        defaultValue = "move",
        shortDescription = "The type of modification to apply.")
@StringSetting(key = "source", displayName = "Source Variable", pos = 2,
        autoCompleteType = AutoCompleteType.VARIABLES,
        shortDescription = "The (sub)variable to modify.")
@StringSetting(key = "target", displayName = "Target Variable", pos = 3,
        subPointer = "mode", subValues = { "move" },
        shortDescription = "The target (sub)variable of the operation.")
@SuppressWarnings("unused")
public class VariableModifyActivity implements VariableWriter {

    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) throws Exception {
        String mode = settings.getString( "mode" );
        String source = settings.getString( "source" );
        String target = settings.getString( "target" );

        String json = mapper.writeValueAsString( ctx.getVariableStore().getPublicVariables( false, false ) );
        PolyDocument doc = PolyValue.fromJson( json ).asDocument();
        DocModifyActivity.modify( doc, mode, source, target );
        writer.removeDynamicVariables( false );
        for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
            writer.setVariable( entry.getKey().value, entry.getValue() );
        }
    }

}
