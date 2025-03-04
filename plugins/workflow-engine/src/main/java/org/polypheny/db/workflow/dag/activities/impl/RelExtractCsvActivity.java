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

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import com.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.activities.impl.RelCreateActivity.EscapeChar;
import org.polypheny.db.workflow.dag.activities.impl.RelCreateActivity.Separator;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "relExtractCsv", displayName = "Extract CSV", categories = { ActivityCategory.EXTRACT, ActivityCategory.RELATIONAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL, description = "The extracted table.") },
        shortDescription = "Extracts a table from one or multiple CSV files stored locally or remotely.")
@DefaultGroup(subgroups = { @Subgroup(key = "format", displayName = "CSV Format") })
@FileSetting(key = "file", displayName = "File Location", pos = 0,
        multi = true,
        shortDescription = "Select the location of the file(s) to extract. In case of multiple files, the union of their rows is computed.")
@BoolSetting(key = "nameCol", displayName = "Add File Name Column", pos = 1)

@EnumSetting(key = "sep", displayName = "Separator", pos = 2, subGroup = "format",
        options = { "COMMA", "SEMICOLON", "TAB", "PIPE", "SPACE", "COLON", "TILDE" },
        displayOptions = { "Comma (',')", "Semicolon (';')", "Tab", "Pipe ('|')", "Space", "Colon (':')", "Tilde ('~')" },
        defaultValue = "COMMA", shortDescription = "The delimiter that separates individual fields.")
@BoolSetting(key = "header", displayName = "First Row is Header", pos = 3, subGroup = "format",
        defaultValue = false,
        shortDescription = "If true, the values of the first row are used as column names. For missing or invalid names, a valid name is derived."
)
@BoolSetting(key = "inferType", displayName = "Infer Column Types", pos = 4, subGroup = "format",
        defaultValue = true,
        shortDescription = "If true, the column types are inferred from the first (non-header) row. Otherwise, all columns are textual."
)

// advanced
@StringSetting(key = "quote", displayName = "Quote Character", pos = 10, group = ADVANCED_GROUP,
        nonBlank = true, minLength = 1, maxLength = 2, defaultValue = "\"",
        shortDescription = "The character for explicitly quoting strings. To use the quote character within a string, it has to be written twice.")
@EnumSetting(key = "escape", displayName = "Escape Character", pos = 11, group = ADVANCED_GROUP,
        options = { "BACKSLASH", "QUOTE", "SINGLE_QUOTE" },
        displayOptions = { "Backslash ('\\')", "Double Quote ('\"')", "Single Quote (''')" },
        defaultValue = "BACKSLASH", shortDescription = "The character for escaping a separator or quote outside of a quoted context.")
@BoolSetting(key = "emptyIsNull", displayName = "Interpret Empty Fields as Null", pos = 12, group = ADVANCED_GROUP,
        defaultValue = true,
        shortDescription = "If true, an empty field is interpreted as a missing value instead of an empty string."
)
@BoolSetting(key = "fail", displayName = "Fail on Invalid Value", pos = 13, group = ADVANCED_GROUP,
        defaultValue = false, subPointer = "inferType", subValues = { "true" },
        shortDescription = "If true, a value that cannot be casted to the inferred column type results in the activity to fail. Otherwise, null is inserted."
)
@BoolSetting(key = "preview", displayName = "Preview Type", pos = 14, group = ADVANCED_GROUP,
        defaultValue = true,
        shortDescription = "If true, the first lines of the file are fetched before the actual execution to preview the output type."
)

@SuppressWarnings("unused")
public class RelExtractCsvActivity implements Activity, Pipeable {

    private List<Source> sources;
    private static final Set<String> EXTENSIONS = Set.of( "csv", "tsv", "txt" );


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.allPresent() && settings.getBool( "preview" ) ) {
            FileValue file = settings.getOrThrow( "file", FileValue.class );
            try {
                sources = file.getSources( EXTENSIONS );
                return RelType.of( getType( sources.get( 0 ), settings.toSettings() ) ).asOutTypes();
            } catch ( Exception e ) {
                throw new InvalidSettingException( "Invalid location: " + e.getMessage(), "file" );
            }
        }
        return UnknownType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        FileValue file = settings.get( "file", FileValue.class );
        sources = file.getSources( EXTENSIONS );
        return getType( sources.get( 0 ), settings );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        boolean addNameCol = settings.getBool( "nameCol" );
        boolean fail = settings.getBool( "fail" );
        PolyValue pkVal = PolyLong.of( 0 );
        AlgDataType type = output.getType();
        int count = type.getFieldCount() - (addNameCol ? 2 : 1); // -1 for pkCol
        List<PolyType> types = type.getFields().stream().map( f -> f.getType().getPolyType() ).toList();
        for ( Source source : sources ) {
            String name = ActivityUtils.resourceNameFromSource( source );
            PolyString polyName = PolyString.of( name );
            ctx.logInfo( "Extracting " + name );

            try ( CSVReader reader = getCSVReader( source.openStream(), settings, true ) ) {
                String[] row = reader.readNext();
                while ( row != null ) {
                    if ( row.length < count ) {
                        row = reader.readNext();
                        continue; // possibly empty line
                    }
                    List<PolyValue> outRow = new ArrayList<>();
                    outRow.add( pkVal );
                    for ( int i = 0; i < count; i++ ) {
                        if ( row[i] == null ) {
                            outRow.add( PolyNull.NULL );
                        } else {
                            try {
                                outRow.add( ActivityUtils.stringToPolyValue( row[i], types.get( i + 1 ) ) );
                            } catch ( Exception e ) {
                                if ( fail ) {
                                    throw new GenericRuntimeException( "Unsuccessful cast of value '" + row[i] + "': " + e.getMessage() );
                                }
                                outRow.add( PolyNull.NULL );
                            }
                        }
                    }
                    if ( addNameCol ) {
                        outRow.add( polyName );
                    }
                    if ( !output.put( outRow ) ) {
                        return;
                    }
                    row = reader.readNext();
                }
            }
        }
    }


    @Override
    public void reset() {
        sources = null;
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return 1_000_000; // an arbitrary estimate to show some progress for larger files
    }


    private AlgDataType getType( Source source, Settings settings ) throws Exception {
        try ( CSVReader reader = getCSVReader( source.openStream(), settings, false ) ) {
            List<String[]> firstRows = RelCreateActivity.getRawRows( reader, 2 );
            AlgDataType type = RelCreateActivity.getType( firstRows, settings.getBool( "header" ), settings.getBool( "inferType" ) );
            if ( settings.getBool( "nameCol" ) ) {
                type = ActivityUtils.appendField( type, "file_name", factory.createPolyType( PolyType.VARCHAR, 255 ) );
            }
            return type;
        }
    }


    private CSVReader getCSVReader( InputStream inputStream, Settings settings, boolean dataOnly ) {
        BufferedReader bufferedReader = new BufferedReader( new InputStreamReader( inputStream, StandardCharsets.UTF_8 ) );
        char sep = Separator.valueOf( settings.getString( "sep" ) ).sep;
        boolean hasHeader = settings.getBool( "header" );
        boolean inferType = settings.getBool( "inferType" );
        char quote = settings.getString( "quote" ).charAt( 0 );
        char escape = EscapeChar.valueOf( settings.getString( "escape" ) ).escape;
        boolean emptyIsNull = settings.getBool( "emptyIsNull" );
        return ActivityUtils.openCSVReader( bufferedReader, sep, quote, escape, dataOnly && hasHeader ? 1 : 0, emptyIsNull );
    }

}
