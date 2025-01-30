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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "varToDoc", displayName = "Write Variables to Document", categories = { ActivityCategory.VARIABLES, ActivityCategory.DOCUMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "Write the current variables (excluding environment variables) to a document."
)
@BoolSetting(key = "includeWf", displayName = "Include Workflow Variables")

@SuppressWarnings("unused")
public class VariableToDocumentActivity implements Activity {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Map<String, JsonNode> variables = ctx.getVariableStore().getPublicVariables( settings.getBool( "includeWf" ), false );

        Map<PolyString, PolyValue> map = new HashMap<>();
        for ( Entry<String, JsonNode> entry : variables.entrySet() ) {
            map.put( PolyString.of( entry.getKey() ), PolyValue.fromJson( entry.getValue().toString() ) );
        }

        DocWriter writer = ctx.createDocWriter( 0 );
        if ( !map.isEmpty() ) {
            writer.write( new PolyDocument( map ) );
        }
    }

}
