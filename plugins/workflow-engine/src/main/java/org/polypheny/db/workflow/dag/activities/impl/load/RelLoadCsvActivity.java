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

import com.opencsv.CSVWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
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
import org.polypheny.db.workflow.engine.storage.StorageManager;

@ActivityDefinition(type = "relLoadCsv", displayName = "Load Table to CSV File", categories = { ActivityCategory.LOAD, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = {},
        shortDescription = "Writes the input table to either a new or an existing CSV file.")

@FileSetting(key = "file", displayName = "Target File", pos = 0,
        multi = false, modes = { SourceType.ABS_FILE },
        shortDescription = "Select the target file.")
@EnumSetting(key = "mode", displayName = "Handling of Existing File", pos = 1,
        options = { "drop", "append", "fail" },
        displayOptions = { "Overwrite", "Append Rows", "Fail Activity" },
        defaultValue = "fail", style = EnumStyle.RADIO_BUTTON,
        shortDescription = "Define activity behavior if the selected file already exists.")
@BoolSetting(key = "keepPk", displayName = "Keep Primary Key Column", pos = 2,
        shortDescription = "Keep the '" + StorageManager.PK_COL + "' column.", defaultValue = false)

@SuppressWarnings("unused")
public class RelLoadCsvActivity implements Activity, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "keepPk" ) && inTypes.get( 0 ) instanceof RelType relType ) {
            if ( relType.getNullableType().getFieldCount() < 2 && !settings.getBool( "keepPk" ) ) {
                throw new InvalidInputException( "Input table must have at least 1 additional column if the primary key column is not kept", 0 );
            }
        }
        return List.of();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        InputPipe input = inputs.get( 0 );
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );
        File file = settings.get( "file", FileValue.class ).getFile( false, false );
        String mode = settings.getString( "mode" );
        boolean keepPkCol = settings.getBool( "keepPk" );
        List<String> header = input.getType().getFieldNames();
        if ( !keepPkCol ) {
            header = header.subList( 1, header.size() );
        }

        boolean fileExists = file.exists();
        boolean writeHeader = true;
        switch ( mode ) {
            case "fail" -> {
                if ( fileExists ) {
                    throw new GenericRuntimeException( "Specified file already exists." );
                }
            }
            case "drop" -> {
                if ( fileExists ) {
                    if ( !file.delete() ) {
                        throw new GenericRuntimeException( "Failed to delete existing file." );
                    }
                }
            }
            case "append" -> {
                if ( fileExists ) {
                    // Check if header matches
                    try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) ) {
                        String existingHeader = reader.readLine();
                        writeHeader = existingHeader == null;
                        if ( existingHeader != null && !existingHeader.equals( String.join( ",", header ) ) ) {
                            throw new GenericRuntimeException( "Header mismatch in append mode with existing file: " + existingHeader );
                        }
                    }
                }
            }
            default -> throw new IllegalArgumentException( "Unknown mode: " + mode );
        }

        try ( CSVWriter writer = new CSVWriter(
                new FileWriter( file, mode.equalsIgnoreCase( "append" ) ),
                CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.DEFAULT_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END
        ) ) {
            if ( writeHeader ) {
                writer.writeNext( header.toArray( new String[0] ), false );
            }

            long rowCount = 0;
            long countDelta = Math.max( estimatedTupleCount / 100, 1 );
            for ( List<PolyValue> row : input ) {
                String[] arrRow;
                if ( keepPkCol ) {
                    arrRow = row.stream().map( ActivityUtils::valueToString ).toArray( String[]::new );
                    arrRow[0] = String.valueOf( rowCount );
                } else {
                    arrRow = row.subList( 1, row.size() ).stream().map( ActivityUtils::valueToString ).toArray( String[]::new );
                }
                writer.writeNext( arrRow, false );
                rowCount++;

                if ( rowCount % countDelta == 0 ) {
                    double progress = (double) rowCount / estimatedTupleCount;
                    ctx.updateProgress( progress );
                    ctx.checkPipeInterrupted();
                }
            }
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
                return "Load Table to " + name;

            } catch ( Exception ignored ) {
            }
        }
        return null;
    }

}
