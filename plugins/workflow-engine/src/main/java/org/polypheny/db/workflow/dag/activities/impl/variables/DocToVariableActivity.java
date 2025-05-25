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

package org.polypheny.db.workflow.dag.activities.impl.variables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;

@ActivityDefinition(type = "docToVar", displayName = "Document to Variable", categories = { ActivityCategory.VARIABLES, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC) },
        outPorts = {},
        shortDescription = "Transform the first document (or its fields) into variables."
)

@BoolSetting(key = "skipId", displayName = "Skip Identifier", defaultValue = true, pos = 0,
        shortDescription = "Do not include the '" + DocumentType.DOCUMENT_ID + "' field.")
@BoolSetting(key = "useDocument", displayName = "Map entire Document", defaultValue = false, pos = 1,
        shortDescription = "If true, the entire document becomes a single variable.")

@StringSetting(key = "docVarName", displayName = "Variable Name", minLength = 1, maxLength = 128, nonBlank = true, defaultValue = "doc", pos = 2,
        shortDescription = "The name of the variable to be generated.", subPointer = "useDocument", subValues = { "true" })

@StringSetting(key = "varPrefix", displayName = "Variable Prefix", minLength = 0, maxLength = 128, pos = 2,
        shortDescription = "An optional prefix to append to the field names.", subPointer = "useDocument", subValues = { "false" })
@FieldSelectSetting(key = "fields", displayName = "Select Fields", reorder = false, pos = 3, defaultUnspecified = true,
        shortDescription = "Select the fields that get written to variables.", subPointer = "useDocument", subValues = { "false" })

@SuppressWarnings("unused")
public class DocToVariableActivity implements VariableWriter {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) {
        DocReader reader = (DocReader) inputs.get( 0 );
        for ( PolyDocument doc : reader.getDocIterable() ) {
            if ( settings.getBool( "skipId" ) ) {
                Map<PolyString, PolyValue> map = new HashMap<>( doc.map );
                map.remove( docId );
                doc = PolyDocument.ofDocument( map );
            }

            if ( settings.getBool( "useDocument" ) ) {
                writeDocument( doc, settings, writer );
            } else {
                writeFields( doc, settings, writer );
            }
            break; // only write the first document
        }
    }


    private void writeDocument( PolyDocument doc, Settings settings, WritableVariableStore writer ) {
        String variableName = settings.getString( "docVarName" );
        writer.setVariable( variableName, doc );

    }


    private void writeFields( PolyDocument doc, Settings settings, WritableVariableStore writer ) {
        String prefix = settings.getString( "varPrefix" );
        FieldSelectValue select = settings.get( "fields", FieldSelectValue.class );
        for ( Map.Entry<PolyString, PolyValue> entry : doc.map.entrySet() ) {
            String key = entry.getKey().value;
            if ( select == null || select.isSelected( key ) ) {
                writer.setVariable( prefix + key, entry.getValue() );
            }
        }
    }

}
