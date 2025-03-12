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

package org.polypheny.db.workflow.dag.activities.impl.extract;

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import com.opencsv.CSVReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Pair;
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
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;


@ActivityDefinition(type = "relCreate", displayName = "Create Table", categories = { ActivityCategory.EXTRACT, ActivityCategory.RELATIONAL, ActivityCategory.ESSENTIALS, ActivityCategory.DEVELOPMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "This activity can be used to create a table from a given csv string. It should only be used for small tables, as the data is stored in the workflow itself."
)
@StringSetting(key = "csv", displayName = "CSV", pos = 0,
        textEditor = true, language = "csv", nonBlank = true, maxLength = 60 * 1024, // TODO: limit size according to websocket message size limit (~64 KB)
        shortDescription = "A table specified in CSV notation. Its details can be further customized in the settings below.")
@EnumSetting(key = "sep", displayName = "Separator", pos = 1,
        options = { "COMMA", "SEMICOLON", "TAB", "PIPE", "SPACE", "COLON", "TILDE" },
        displayOptions = { "Comma (',')", "Semicolon (';')", "Tab", "Pipe ('|')", "Space", "Colon (':')", "Tilde ('~')" },
        defaultValue = "COMMA", shortDescription = "The delimiter that separates individual fields.")
@BoolSetting(key = "header", displayName = "First Row is Header", pos = 2,
        defaultValue = true,
        shortDescription = "If true, the values of the first row are used as column names. For missing or invalid names, a fallback value is used."
)
@BoolSetting(key = "inferType", displayName = "Infer Column Types", pos = 3,
        defaultValue = true,
        shortDescription = "If true, the column types are inferred from the first (non-header) row. Otherwise, all columns are textual."
)

// advanced
@StringSetting(key = "quote", displayName = "Quote Character", pos = 4, group = ADVANCED_GROUP,
        nonBlank = true, minLength = 1, maxLength = 2, defaultValue = "\"",
        shortDescription = "The character for explicitly quoting strings. To use the quote character within a string, it has to be written twice.")
@EnumSetting(key = "escape", displayName = "Escape Character", pos = 5, group = ADVANCED_GROUP,
        options = { "BACKSLASH", "QUOTE", "SINGLE_QUOTE" },
        displayOptions = { "Backslash ('\\')", "Double Quote ('\"')", "Single Quote (''')" },
        defaultValue = "BACKSLASH", shortDescription = "The character for escaping a separator or quote outside of a quoted context.")
@BoolSetting(key = "emptyIsNull", displayName = "Interpret Empty Fields as Null", pos = 6, group = ADVANCED_GROUP,
        defaultValue = true,
        shortDescription = "If true, an empty field is interpreted as a missing value instead of an empty string."
)
@BoolSetting(key = "fail", displayName = "Fail on Invalid Value", pos = 8, group = ADVANCED_GROUP,
        defaultValue = false, subPointer = "inferType", subValues = { "true" },
        shortDescription = "If true, a value that cannot be casted to the inferred column type results in the activity to fail. Otherwise, null is inserted."
)

@SuppressWarnings("unused")
public class RelCreateActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.allPresent() ) {
            String csv = settings.getString( "csv" );
            char sep = Separator.valueOf( settings.getString( "sep" ) ).sep;
            boolean hasHeader = settings.getBool( "header" );
            boolean inferType = settings.getBool( "inferType" );
            char quote = settings.getString( "quote" ).charAt( 0 );
            char escape = EscapeChar.valueOf( settings.getString( "escape" ) ).escape;
            boolean emptyIsNull = settings.getBool( "emptyIsNull" );

            try ( CSVReader reader = ActivityUtils.openCSVReader( new StringReader( csv ), sep, quote, escape, 0, emptyIsNull ) ) {
                List<String[]> firstRows = getRawRows( reader, 2 );
                return RelType.of( getType( firstRows, hasHeader, inferType ) ).asOutTypes();
            } catch ( InvalidSettingException e ) {
                throw e;
            } catch ( Exception e ) {
                throw new InvalidSettingException( "Error while reading csv: " + e.getMessage(), "csv" );
            }
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        try ( CSVReader reader = getReader( settings, false ) ) {
            List<String[]> firstRows = getRawRows( reader, 2 );
            return getType( firstRows, settings.getBool( "header" ), settings.getBool( "inferType" ) );
        }
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        List<PolyType> types = output.getType().getFields().stream().map( f -> f.getType().getPolyType() ).toList();
        PolyValue pkVal = PolyLong.of( 0 );
        int count = output.getType().getFieldCount() - 1; // minus pkCol
        boolean fail = settings.getBool( "fail" );
        try ( CSVReader reader = getReader( settings, true ) ) {
            String[] row = reader.readNext();
            while ( row != null ) {
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
                if ( !output.put( outRow ) ) {
                    return;
                }
                row = reader.readNext();
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        int newlineCount = settings.getBool( "header" ) ? 0 : 1; // assuming no trailing newline
        int index = 0;

        String csv = settings.getString( "csv" );
        while ( (index = csv.indexOf( '\n', index )) != -1 ) {
            newlineCount++;
            index++;
        }
        return newlineCount;
    }


    private CSVReader getReader( Settings settings, boolean dataOnly ) {
        String csv = settings.getString( "csv" );
        char sep = Separator.valueOf( settings.getString( "sep" ) ).sep;
        boolean hasHeader = settings.getBool( "header" );
        boolean inferType = settings.getBool( "inferType" );
        char quote = settings.getString( "quote" ).charAt( 0 );
        char escape = EscapeChar.valueOf( settings.getString( "escape" ) ).escape;
        boolean emptyIsNull = settings.getBool( "emptyIsNull" );
        return ActivityUtils.openCSVReader( new StringReader( csv ), sep, quote, escape, dataOnly && hasHeader ? 1 : 0, emptyIsNull );
    }


    public static List<String[]> getRawRows( CSVReader reader, int maxRows ) throws Exception {
        List<String[]> rows = new ArrayList<>();
        if ( maxRows > 0 ) {
            for ( int i = 0; i < maxRows; i++ ) {
                String[] row = reader.readNext();
                if ( row == null ) {
                    break;
                }
                rows.add( row );
            }
        } else {
            rows = reader.readAll();
        }
        if ( rows.isEmpty() ) {
            throw new InvalidSettingException( "No rows found", "csv" );
        }
        if ( rows.get( 0 ).length < 1 ) {
            throw new InvalidSettingException( "No columns found", "csv" );
        }
        return rows;
    }


    public static AlgDataType getType( List<String[]> rawRows, boolean hasHeader, boolean inferType ) throws InvalidSettingException {
        String[] firstRow = rawRows.get( 0 );
        int count = firstRow.length;

        List<String> colNames = new ArrayList<>();
        for ( int i = 0; i < count; i++ ) {
            if ( hasHeader ) {
                String name = ActivityUtils.deriveValidFieldName( firstRow[i], "col", i );
                colNames.add( name ); // we allow duplicate columns (they get uniquified)
            } else {
                colNames.add( "col" + i );
            }
        }

        List<AlgDataType> colTypes = new ArrayList<>();
        if ( inferType ) {
            if ( hasHeader && rawRows.size() < 2 ) {
                throw new InvalidSettingException( "Not enough rows to infer column types", "inferType" );
            }
            for ( String value : rawRows.get( hasHeader ? 1 : 0 ) ) {
                PolyType polyType = ActivityUtils.inferPolyType( value, PolyType.TEXT );
                colTypes.add( factory.createPolyType( polyType ) );
            }
        } else {
            for ( int i = 0; i < count; i++ ) {
                colTypes.add( factory.createPolyType( PolyType.TEXT ) );
            }
        }
        Builder builder = ActivityUtils.getBuilder();
        for ( Pair<String, AlgDataType> pair : Pair.zip( colNames, colTypes ) ) {
            builder.add( pair.left, null, pair.right ).nullable( true );
        }
        return ActivityUtils.addPkCol( builder.build() );
    }


    public enum Separator {
        COMMA( ',' ),
        SEMICOLON( ';' ),
        TAB( '\t' ),
        PIPE( '|' ),
        SPACE( ' ' ),
        COLON( ':' ),
        TILDE( '~' );

        public final char sep;


        Separator( char sep ) {
            this.sep = sep;
        }

    }


    public enum EscapeChar {
        QUOTE( '"' ),
        SINGLE_QUOTE( '\'' ),
        BACKSLASH( '\\' );

        public final char escape;


        EscapeChar( char escape ) {
            this.escape = escape;
        }

    }

}
