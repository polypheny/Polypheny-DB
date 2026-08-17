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

package org.polypheny.db.workflow.dag.activities.impl.extract;

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.parquet.ParquetWorkflowExtractSupport;

/**
 * Workflow node definition for reading Parquet files into a workflow.
 * Fulfills the following functions:
 * 1. defines how the activity appears in the workflow UI.
 *    It declares the name, description, category, settings, and output port.
 *    So, the workflow editor can show an Extract Parquet node with options.
 * 2. decides what type of data the node will output (document or relational)
 * 3. executes the extraction at runtime.
 */
@ActivityDefinition(type = "extractParquet", displayName = "Extract Parquet", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.RELATIONAL, ActivityCategory.ESSENTIALS, ActivityCategory.EXTERNAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.ANY, description = "The extracted Parquet data.") },
        shortDescription = "Extracts one or multiple Parquet files into either a document collection or a relational table.")
@FileSetting(key = "file", displayName = "File Location", pos = 0,
        multi = true, modes = { SourceType.ABS_FILE, SourceType.URL },
        shortDescription = "Select the Parquet file or folder to extract. In case of multiple files, the union of their rows is computed.")
@EnumSetting(key = "outputModel", displayName = "Output Type", pos = 1,
        options = { ParquetWorkflowExtractSupport.OUTPUT_DOCUMENT, ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL },
        displayOptions = { "Document", "Relational" },
        defaultValue = ParquetWorkflowExtractSupport.OUTPUT_DOCUMENT,
        shortDescription = "Choose whether the Parquet rows should be exposed as documents or as a relational table.")
@BoolSetting(key = "nameField", displayName = "Add File Name Field", pos = 2,
        shortDescription = "Adds the source file name as a field or column in the output.")
@IntSetting(key = "maxCount", displayName = "Maximum Row Count", defaultValue = -1, min = -1, pos = 3, group = ADVANCED_GROUP,
        shortDescription = "The maximum number of rows to extract per file or -1 to extract all rows.")
@SuppressWarnings("unused")
public class ParquetExtractActivity implements Activity, Pipeable {

    private static final Set<String> EXTENSIONS = ParquetWorkflowExtractSupport.EXTENSIONS;
    private List<Source> sources;


    /**
     * Check what node will output:
     * - is the output document or relational
     * - if relational, what columns and types should we expect
     * - if we can’t know yet, should we show an unknown type
     * @param inTypes a list of {@link TypePreview}s representing the input tuple types.
     * @param settings the SettingsPreview representing the available settings, i.e. all settings that do not contain variables.
     * @return List<TypePreview>
     * @throws ActivityException - exception
     */
    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( !settings.keysPresent( "outputModel" ) ) {
            // outputModel is not set yet -> UnknownType
            return UnknownType.of().asOutTypes();
        }

        String outputModel = settings.getString( "outputModel" );
        // Document -> no schema
        if ( ParquetWorkflowExtractSupport.OUTPUT_DOCUMENT.equals( outputModel ) ) {
            if ( settings.keysPresent( "nameField" ) && settings.getBool( "nameField" ) ) {
                return DocType.of( Set.of( "fileName" ) ).asOutTypes();
            }
            return DocType.of().asOutTypes();
        }

        // Relational -> need schema
        if ( settings.keysPresent( "file" ) ) {
            // inspect selected file
            try {
                List<Source> previewSources = settings.getOrThrow( "file", FileValue.class ).getSources( EXTENSIONS );
                if ( previewSources.isEmpty() ) {
                    // no files found
                    throw new InvalidSettingException( "No parquet files found", "file" );
                }
                // allow multiple relational sources if all parquet schemas are identical
                ParquetWorkflowExtractSupport.validateSharedRelationalSchema( previewSources );
                boolean addNameField = settings.keysPresent( "nameField" ) && settings.getBool( "nameField" );
                // read the Parquet schema and build a relational preview type
                return RelType.of( ParquetWorkflowExtractSupport.getOutputType( previewSources.get( 0 ), outputModel, addNameField ) ).asOutTypes();
            } catch ( InvalidSettingException e ) {
                throw e;
            } catch ( GenericRuntimeException e ) {
                throw new InvalidSettingException( e.getMessage(), "file" );
            } catch ( Exception e ) {
                throw new InvalidSettingException( "Invalid location: " + e.getMessage(), "file" );
            }
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    /**
     * Decide the final, concrete output schema for the activity before execution starts
     * @param inTypes the types of the input pipes - ignored, because activity has no input ports
     * @param settings the resolved settings
     * @return AlgDataType
     * @throws Exception - exception
     */
    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        sources = settings.get( "file", FileValue.class ).getSources( EXTENSIONS );
        if ( sources.isEmpty() ) {
            throw new InvalidSettingException( "No parquet files found", "file" );
        }
        if ( ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL.equals( settings.getString( "outputModel" ) ) ) {
            ParquetWorkflowExtractSupport.validateSharedRelationalSchema( sources );
        }
        return ParquetWorkflowExtractSupport.getOutputType( sources.get( 0 ), settings.getString( "outputModel" ), settings.getBool( "nameField" ) );
    }


    /**
     * Produce the rows/documents and sends them to the activity’s output port
     * @param inputs the InputPipes to iterate over. For inactive edges, the pipe is null (important for non-default DataStateMergers).
     * @param output the output pipe for sending output tuples to that respect the locked output type, or null if this activity has no output
     * @param settings the resolved settings
     * @param ctx ExecutionContext to be used for access to the transaction (interrupt checking is done automatically by the pipes)
     * @throws Exception exception
     */
    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        String outputModel = settings.getString( "outputModel" );
        boolean addNameField = settings.getBool( "nameField" );
        int maxCount = settings.getInt( "maxCount" );

        for ( Source source : sources ) {
            ctx.logInfo( "Extracting " + source.path() + " as " + outputModel );
            // write for the next stage in pipe
            if ( ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL.equals( outputModel ) ) {
                ParquetWorkflowExtractSupport.writeRows( output, source, addNameField, maxCount );
            } else {
                ParquetWorkflowExtractSupport.writeDocuments( output, source, addNameField, maxCount );
            }
        }
    }


    @Override
    public void reset() {
        sources = null;
    }


    /**
     * Compute a more specific display name for the node based on the current settings
     * @param inTypes a list of {@link TypePreview}s representing the input tuple types.
     * @param settings the SettingsPreview representing the available settings, i.e. all settings that do not contain variables.
     * @return String name
     */
    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "file" ) && settings.keysPresent( "outputModel" ) ) {
            try {
                List<Source> previewSources = settings.getOrThrow( "file", FileValue.class ).getSources( EXTENSIONS );
                if ( !previewSources.isEmpty() ) {
                    // if file data found generate more specific activity name
                    return ParquetWorkflowExtractSupport.getDynamicName( settings.getString( "outputModel" ), previewSources );
                }
            } catch ( Exception ignored ) {
            }
        }
        return null;
    }


    /**
     * Gives the workflow engine a rough estimate of how many output rows this activity will produce
     * @param inTypes the types of the inputs. For inactive edges, the entry is null (important for non-default DataStateMergers).
     * @param settings the resolved settings
     * @param inCounts the list of input tuple counts. -1 if the estimation is not possible and null for inactive edges.
     * @param transactionSupplier to be used for access to a transaction
     * @return long
     */
    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        try {
            // check if sources is available
            if ( sources == null ) {
                // resolves the files from settings
                sources = settings.get( "file", FileValue.class ).getSources( EXTENSIONS );
            }
            return ParquetWorkflowExtractSupport.estimateTupleCount( sources );
        } catch ( Exception e ) {
            return -1;
        }
    }

}
