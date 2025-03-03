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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.text.StringSubstitutor;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "varStringTemplate", displayName = "String Variable from Template", categories = { ActivityCategory.TRANSFORM, ActivityCategory.VARIABLES },
        inPorts = {},
        outPorts = {},
        shortDescription = "Define a dynamic string variable based on a template that may use other variables."
)

@StringSetting(key = "template", displayName = "Template", pos = 0,
        textEditor = true, language = "plain_text",
        shortDescription = "The string to insert. Can use the template expression '{varName}' to insert other variables. Sub-variables can be specified as JsonPointers.")
@StringSetting(key = "target", displayName = "Target Variable", pos = 1,
        shortDescription = "The target (sub)variable.")
@BoolSetting(key = "fail", displayName = "Fail on Non-Literal Variable", pos = 2,
        shortDescription = "Whether the execution should fail if the target (sub)variable is not a JSON literal. Otherwise, its JSON representation is inserted.")
@SuppressWarnings("unused")
public class VariableStringTemplateActivity implements VariableWriter {

    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) throws Exception {
        String template = settings.getString( "template" );
        String target = settings.getString( "target" );
        boolean fail = settings.getBool( "fail" );

        Map<String, JsonNode> variables = ctx.getVariableStore().getPublicVariables( true, false );
        StringSubstitutor substitutor = new StringSubstitutor( v -> resoleVariable( variables, v, fail ), "{", "}", '\\' )
                .setDisableSubstitutionInValues( true );
        String resolved = substitutor.replace( template );

        String json = mapper.writeValueAsString( ctx.getVariableStore().getDynamicVariables() );
        PolyDocument doc = PolyValue.fromJson( json ).asDocument();
        DocModifyActivity.modify( doc, "setValue", target, resolved );
        writer.removeDynamicVariables( false );
        for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
            writer.setVariable( entry.getKey().value, entry.getValue() );
        }
    }


    private String resoleVariable( Map<String, JsonNode> variables, String refString, boolean fail ) {
        if ( refString.startsWith( "/" ) ) {
            refString = refString.substring( 1 );
        }
        String[] refSplit = refString.split( "/", 2 );
        String variableRef = refSplit[0].replace( JsonPointer.ESC_SLASH, "/" ).replace( JsonPointer.ESC_TILDE, "~" );
        JsonNode replacement = variables.get( variableRef );
        if ( replacement != null && refSplit.length == 2 && !refSplit[1].isEmpty() ) {
            replacement = replacement.at( "/" + refSplit[1] ); // resolve JsonPointer
        }
        if ( replacement == null || replacement.isMissingNode() ) {
            throw new GenericRuntimeException( "Variable pointer cannot be resolved: " + refString, "template" );
        }
        if ( fail && !replacement.isValueNode() ) {
            throw new GenericRuntimeException( "Variable for reference '" + refString + "' to insert is not a JSON literal: " + replacement );
        }
        return replacement.isTextual() ? replacement.asText() : replacement.toString();
    }

}
