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

package org.polypheny.db.workflow.dag.activities.impl.load;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

@ActivityDefinition(type = "docLoadJson", displayName = "Load Collection to JSON File", categories = { ActivityCategory.LOAD, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC) },
        outPorts = {},
        shortDescription = "Writes the input collection to either a new or an existing JSON file.")

@FileSetting(key = "file", displayName = "Target File", pos = 0,
        multi = false, modes = { SourceType.ABS_FILE },
        shortDescription = "Select the target file.")
@EnumSetting(key = "mode", displayName = "Handling of Existing File", pos = 1,
        options = { "drop", "fail" },
        displayOptions = { "Overwrite", "Fail Activity" },
        defaultValue = "fail", style = EnumStyle.RADIO_BUTTON,
        shortDescription = "Define activity behavior if the selected file already exists.")
@BoolSetting(key = "keepId", displayName = "Include ID Field", pos = 2,
        shortDescription = "Keep the '" + DocumentType.DOCUMENT_ID + "' field.", defaultValue = true)

@SuppressWarnings("unused")
public class DocLoadJsonActivity implements Activity, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );
        File file = settings.get( "file", FileValue.class ).getFile( false, false );
        String mode = settings.getString( "mode" );
        boolean keepId = settings.getBool( "keepId" );

        if ( file.exists() ) {
            switch ( mode ) {
                case "fail" -> throw new GenericRuntimeException( "Specified file already exists." );
                case "drop" -> {
                    if ( !file.delete() ) {
                        throw new GenericRuntimeException( "Failed to delete existing file." );
                    }
                }
            }
        }

        long docCount = 0;
        long countDelta = Math.max( estimatedTupleCount / 100, 1 );

        try ( FileWriter writer = new FileWriter( file );
                JsonGenerator jsonGenerator = new ObjectMapper().createGenerator( writer ) ) {

            jsonGenerator.writeStartArray(); // Start the JSON array

            // Write the data
            for ( List<PolyValue> row : inputs.get( 0 ) ) {
                PolyDocument doc = row.get( 0 ).asDocument();
                if ( !keepId ) {
                    doc.remove( docId );
                }
                // This way we do not have the type information we would get with
                // PolyValue.JSON_WRAPPER.writeValue( jsonGenerator, doc )
                jsonGenerator.writeRawValue( doc.toJson() );
                docCount++;

                if ( docCount % countDelta == 0 ) {
                    double progress = (double) docCount / estimatedTupleCount;
                    ctx.updateProgress( progress );
                    ctx.checkPipeInterrupted();
                }
            }

            jsonGenerator.writeEndArray();
            jsonGenerator.flush();
        }
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "file" ) ) {
            FileValue file = settings.getOrThrow( "file", FileValue.class );
            try {
                String name = file.getFile( false, false ).getName();
                if ( name.length() > 40 ) {
                    name = name.substring( 0, 37 ) + "...";
                }
                return "Load Collection to " + name;

            } catch ( Exception ignored ) {
            }
        }
        return null;
    }

}
