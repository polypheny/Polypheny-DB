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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "setEnvVars", displayName = "Set Environment Variables", categories = { ActivityCategory.VARIABLES },
        inPorts = {},
        outPorts = {},
        shortDescription = "Load Environment Variables from a JSON file."
)
@FileSetting(key = "file", displayName = "File Path", pos = 0,
        multi = false, modes = { SourceType.ABS_FILE, SourceType.URL },
        defaultPath = "classpath://sample_env.json",
        shortDescription = "The path to the JSON file containing environment variables.")

@SuppressWarnings("unused")
public class EnvVariablesActivity implements VariableWriter {

    private final ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) {
        FileValue file = settings.get( "file", FileValue.class );
        try {
            Source source = file.getSource();
            JsonNode root = mapper.readTree( source.openStream() );
            if ( !root.isObject() ) {
                throw new GenericRuntimeException( "The file does not contain a JSON object." );
            }
            ctx.logInfo( "Found " + root.properties().size() + " variables." );

            for ( Entry<String, JsonNode> entry : root.properties() ) {
                ctx.logInfo( "Setting environment variable '" + entry.getKey() + "'" );
                writer.setEnvVariable( entry.getKey(), entry.getValue() );
            }
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Error reading file " + file.getPath() + ": " + e.getMessage() );
        }
    }

}
