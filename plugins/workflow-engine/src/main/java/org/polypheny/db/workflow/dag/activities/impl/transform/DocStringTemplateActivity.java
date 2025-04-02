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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.text.StringSubstitutor;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.impl.variables.VariableStringTemplateActivity;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "docStringTemplate", displayName = "String Field from Template", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC) },
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "Define a dynamic string field based on a template that may use variables and document fields."
)

@StringSetting(key = "template", displayName = "Template", pos = 0,
        textEditor = true, language = "plain_text",
        shortDescription = "The string to insert. Can use the template expression '{varName}' to insert other variables and '{$.field}' for fields. Sub-variables can be specified as JsonPointers, sub-fields using dot-notation.")
@StringSetting(key = "target", displayName = "Target Field", pos = 1,
        nonBlank = true,
        shortDescription = "The target (sub)field.")
@BoolSetting(key = "fail", displayName = "Fail on Non-Literal Values", pos = 2,
        shortDescription = "Whether the execution should fail if the target value is not a (JSON) literal. Otherwise, its JSON representation is inserted.")
@BoolSetting(key = "createNew", displayName = "Create New Document", pos = 2,
        shortDescription = "If true, the target field is added to a new document with the same ID as the corresponding input document.")
@SuppressWarnings("unused")
public class DocStringTemplateActivity implements Activity {

    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "target", "createNew" ) ) {
            Set<String> fields = new HashSet<>();
            if ( !settings.getBool( "createNew" ) && inTypes.get( 0 ) instanceof DocType docType ) {
                fields.addAll( docType.getKnownFields() );
            }
            String target = settings.getString( "target" );
            if ( !target.contains( "." ) ) {
                fields.add( target );
            }
            return DocType.of( fields ).asOutTypes();
        }
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        String template = settings.getString( "template" );
        String target = settings.getString( "target" );
        boolean fail = settings.getBool( "fail" );
        boolean createNew = settings.getBool( "createNew" );

        Map<String, JsonNode> variables = ctx.getVariableStore().getPublicVariables( true, false );

        DocReader reader = (DocReader) inputs.get( 0 );
        DocWriter writer = ctx.createDocWriter( 0 );
        for ( PolyDocument doc : reader.getDocIterable() ) {
            StringSubstitutor substitutor = new StringSubstitutor( v -> resolve( variables, doc, v, fail ), "{", "}", '\\' )
                    .setDisableSubstitutionInValues( true );
            String resolved = substitutor.replace( template );

            PolyDocument docToWrite = createNew ? new PolyDocument( Map.of( docId, doc.get( docId ) ) ) : doc;
            ActivityUtils.insertSubValue( docToWrite, target, PolyString.of( resolved ) );
            writer.write( docToWrite );
        }
    }


    private String resolve( Map<String, JsonNode> variables, PolyDocument doc, String refString, boolean fail ) {
        if ( refString.equals( "$" ) ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Value for reference '" + refString + "' to insert is not a literal value" );
            }
            return doc.toJson(); // entire document
        } else if ( refString.startsWith( "$." ) ) {
            try {
                PolyValue replacement = ActivityUtils.getSubValue( doc, refString.substring( 2 ) );
                if ( replacement == null || replacement.isNull() ) {
                    return null;
                }
                if ( replacement.isString() ) {
                    return replacement.asString().value;
                }
                String json = replacement.toJson();
                if ( fail && !mapper.readTree( json ).isValueNode() ) {
                    throw new GenericRuntimeException( "Value for reference '" + refString + "' to insert is not a literal value: " + replacement );
                }
                return json;
            } catch ( Exception e ) {
                throw new GenericRuntimeException( e );
            }
        } else {
            return VariableStringTemplateActivity.resolveVariable( variables, refString, fail );
        }
    }

}
