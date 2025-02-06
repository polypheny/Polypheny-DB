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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "varToRow", displayName = "Write Variables to Table", categories = { ActivityCategory.VARIABLES, ActivityCategory.RELATIONAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "Write the current variables (excluding environment variables) to a document."
)
@BoolSetting(key = "allJson", displayName = "All JSON")
@BoolSetting(key = "includeWf", displayName = "Include Workflow Variables")

@SuppressWarnings("unused")
public class VariableToRowActivity implements Activity {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return UnknownType.ofRel().asOutTypes(); // type depends on variables
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Map<String, JsonNode> variables = ctx.getVariableStore().getPublicVariables( settings.getBool( "includeWf" ), false );
        boolean allJson = settings.getBool( "allJson" );
        Builder builder = factory.builder().add( StorageManager.PK_FIELD );
        List<PolyValue> row = new ArrayList<>( List.of( PolyInteger.of( 0 ) ) );

        for ( Entry<String, JsonNode> entry : variables.entrySet() ) {
            String key = entry.getKey();
            JsonNode node = entry.getValue();
            PolyValue value;

            if ( allJson ) {
                builder.add( key, null, PolyType.TEXT );
                value = PolyString.of( node.toString() );
            } else if ( node.isInt() ) {
                builder.add( key, null, PolyType.INTEGER );
                value = PolyInteger.of( node.asInt() );
            } else if ( node.isTextual() ) {
                builder.add( key, null, PolyType.VARCHAR, node.toString().length() );
                value = PolyString.of( node.asText() );
            } else {
                builder.add( key, null, PolyType.TEXT );
                value = PolyString.of( node.toString() );
            }
            row.add( value );
        }
        builder.uniquify(); // make sure no conflicts with pk column name

        RelWriter writer = ctx.createRelWriter( 0, builder.build() );
        writer.write( row );
    }

}
