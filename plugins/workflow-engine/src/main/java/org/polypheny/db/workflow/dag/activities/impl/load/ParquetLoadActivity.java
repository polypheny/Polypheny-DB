/*
 * Copyright 2019-2026 The Polypheny Project
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

import java.io.File;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
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
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.parquet.ParquetWorkflowLoadSupport;

/**
 * Workflow node for exporting workflow data into a Parquet file
 * 1. defines UI for activity: accept ANY input port, exposes relevant settings like target file, compression, etc.
 * 2. validates if input supported
 * 3. acts as dispatcher at runtime
 */
@ActivityDefinition(type = "loadParquet", displayName = "Load to Parquet File", categories = { ActivityCategory.LOAD, ActivityCategory.DOCUMENT, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.ANY, description = "Input table or collection") },
        outPorts = {},
        shortDescription = "Writes the input table or collection to a Parquet file.")

@FileSetting(key = "file", displayName = "Target File", pos = 0,
        multi = false, modes = { SourceType.ABS_FILE },
        shortDescription = "Select the target file.")
@EnumSetting(key = "mode", displayName = "Handling of Existing File", pos = 1,
        options = { ParquetWorkflowLoadSupport.MODE_DROP, ParquetWorkflowLoadSupport.MODE_FAIL },
        displayOptions = { "Overwrite", "Fail Activity" },
        defaultValue = ParquetWorkflowLoadSupport.MODE_FAIL, style = EnumStyle.RADIO_BUTTON,
        shortDescription = "Define activity behavior if the selected file already exists.")
@EnumSetting(key = "compression", displayName = "Compression", pos = 2,
        options = { ParquetWorkflowLoadSupport.COMPRESSION_SNAPPY, ParquetWorkflowLoadSupport.COMPRESSION_GZIP, ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED },
        displayOptions = { "Snappy", "Gzip", "Uncompressed" },
        defaultValue = ParquetWorkflowLoadSupport.COMPRESSION_SNAPPY, style = EnumStyle.DROPDOWN,
        shortDescription = "Select the Parquet compression codec.")
@IntSetting(key = "schemaSampleSize", displayName = "Schema Sample Size", pos = 3,
        defaultValue = 100, min = 1,
        shortDescription = "Infer the schema from the first N tuples before writing.")
@EnumSetting(key = "conflictMode", displayName = "Conflict Mode Handling", pos = 4,
        options = { ParquetWorkflowLoadSupport.CONFLICT_STRINGIFY, ParquetWorkflowLoadSupport.CONFLICT_FAIL },
        displayOptions = { "Use Shared String Type", "Fail Activity" },
        defaultValue = ParquetWorkflowLoadSupport.CONFLICT_STRINGIFY, style = EnumStyle.RADIO_BUTTON,
        shortDescription = "Define how incompatible sampled values should be merged during schema inference.")
@BoolSetting(key = "keepId", displayName = "Include ID Field", pos = 5,
        shortDescription = "Keep the '_id' field for document input.", defaultValue = true)
@BoolSetting(key = "keepPk", displayName = "Keep Primary Key Column", pos = 6,
        shortDescription = "Keep the '" + Activity.PK_COL + "' column for relational input.", defaultValue = false)

@SuppressWarnings("unused")
public class ParquetLoadActivity implements Activity, Pipeable {

    private static final int MAX_NAME_LENGTH = 40;
    private static final int TRUNCATED_LENGTH = 37;


    /**
     * Validate input
     * @param inTypes a list of {@link TypePreview}s representing the input tuple types.
     * @param settings the SettingsPreview representing the available settings, i.e. all settings that do not contain variables.
     * @return List<TypePreview>
     * @throws ActivityException - exception
     */
    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview inputType = inTypes.get( 0 );
        // reject graph input, for relational input makes sure the table still
        if ( inputType.getDataModel() == DataModel.GRAPH ) {
            throw new InvalidInputException( "Graph input is not supported for Parquet export.", 0 );
        }
        // for relational input makes sure the table still has at least one non-PK column if keepPk=false
        if ( settings.keysPresent( "keepPk" ) && inputType instanceof RelType relType ) {
            if ( relType.getNullableType().getFieldCount() < 2 && !settings.getBool( "keepPk" ) ) {
                throw new InvalidInputException( "Input table must have at least 1 additional column if the primary key column is not kept", 0 );
            }
        }
        return List.of();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) {
        return null;
    }


    /**
     * Reads settings, resolves the input model,
     * prepares the target file, routes execution to helper class
     * @param inputs the InputPipes to iterate over. For inactive edges, the pipe is null (important for non-default DataStateMergers).
     * @param output the output pipe for sending output tuples to that respect the locked output type, or null if this activity has no output
     * @param settings the resolved settings
     * @param ctx ExecutionContext to be used for access to the transaction (interrupt checking is done automatically by the pipes)
     * @throws Exception - exception
     */
    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        InputPipe input = inputs.get( 0 );
        File file = settings.get( "file", FileValue.class ).getFile( false, false );
        String mode = settings.getString( "mode" );
        String compression = settings.getString( "compression" );
        int schemaSampleSize = settings.getInt( "schemaSampleSize" );
        String conflictMode = settings.getString( "conflictMode" );
        boolean keepId = settings.getBool( "keepId" );
        boolean keepPk = settings.getBool( "keepPk" );
        long estimatedTupleCount = ctx.getEstimatedInCounts().isEmpty() ? -1 : ctx.getEstimatedInCounts().get( 0 );

        ParquetWorkflowLoadSupport.prepareTargetFile( file, mode );

        switch ( ActivityUtils.getDataModel( input.getType() ) ) {
            case DOCUMENT -> {
                ctx.logInfo( "Writing document input to Parquet file: " + file.getAbsolutePath() );
                ParquetWorkflowLoadSupport.writeDocuments( input, file, compression, schemaSampleSize, conflictMode, keepId, estimatedTupleCount, ctx );
            }
            case RELATIONAL -> {
                // prevent producing an empty Parquet schema from relational input
                if ( input.getType().getFieldCount() < 2 && !keepPk ) {
                    throw new InvalidInputException( "Input table must have at least 1 additional column if the primary key column is not kept", 0 );
                }
                ctx.logInfo( "Writing relational input to Parquet file: " + file.getAbsolutePath() );
                ParquetWorkflowLoadSupport.writeRelational( input, file, compression, schemaSampleSize, conflictMode, keepPk, estimatedTupleCount, ctx );
            }
            case GRAPH -> throw new InvalidInputException( "Graph input is not supported for Parquet export.", 0 );
        }
    }


    /**
     * Provides a nicer label in the workflow editor:
     * - gets file name from settings
     * - build dynamic name using lower level functionality
     * @param inTypes a list of {@link TypePreview}s representing the input field types. (all columns in a row)
     * @param settings the SettingsPreview representing the available settings, i.e. all settings that do not contain variables.
     * @return - string
     */
    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "file" ) ) {
            FileValue file = settings.getOrThrow( "file", FileValue.class );
            try {
                return ParquetWorkflowLoadSupport.getDynamicName( file );
            } catch ( Exception ignored ) {
            }
        }
        return null;
    }

}
