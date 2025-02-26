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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
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
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relStringSplit", displayName = "Split String Column", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.REL, description = "A table with a string column to be split") },
        outPorts = { @OutPort(type = PortType.REL, description = "The input table with possibly multiple new string columns containing parts of the string column of the input table.") },
        shortDescription = "Split a string column either by specifying a delimiter or capturing groups using Regex."
)

@StringSetting(key = "column", displayName = "Column to Split", pos = 0,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, nonBlank = true, maxLength = 128,
        shortDescription = "The string column to split.")
@EnumSetting(key = "mode", displayName = "Splitting Mode", pos = 1,
        options = { "delimiter", "pattern" },
        displayOptions = { "Delimiter", "Match Pattern" },
        displayDescriptions = { "Specify a Regex delimiter and a limit.", "Match capturing groups specified using Regex." },
        defaultValue = "delimiter", style = EnumStyle.RADIO_BUTTON)

@StringSetting(key = "delimiter", displayName = "Regex Delimiter", nonBlank = true, pos = 10,
        defaultValue = ",",
        subPointer = "mode", subValues = { "\"delimiter\"" }, containsRegex = true, maxLength = 1024)
@IntSetting(key = "limit", displayName = "Maximum Number of Parts", pos = 11,
        subPointer = "mode", subValues = { "\"delimiter\"" }, defaultValue = 2, min = 1,
        shortDescription = "The number of split columns to add. For a row that results in fewer splits, the remaining columns are set to null.")

@StringSetting(key = "pattern", displayName = "Regex Pattern", nonBlank = true, pos = 100,
        defaultValue = "https?://([^/]+)",
        subPointer = "mode", subValues = { "\"pattern\"" }, containsRegex = true, maxLength = 1024)
@BoolSetting(key = "ignoreCase", displayName = "Ignore Case", pos = 101,
        subPointer = "mode", subValues = { "\"pattern\"" }
)

@BoolSetting(key = "keepCol", displayName = "Keep Original Column", pos = 0,
        group = GroupDef.ADVANCED_GROUP, defaultValue = false
)
@BoolSetting(key = "fail", displayName = "Fail on Invalid Split", pos = 1,
        group = GroupDef.ADVANCED_GROUP, defaultValue = false,
        shortDescription = "If true, a row that cannot be split into enough parts or the pattern cannot be matched results in the activity to fail."
)
@StringSetting(key = "prefix", displayName = "Output Column Prefix", pos = 2,
        maxLength = 128, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "Prefix used for the output columns containing parts of the column to split. If empty, the name of the input column is used as prefix.")
@BoolSetting(key = "matchAll", displayName = "Match Entire String", pos = 3,
        subPointer = "mode", subValues = { "\"pattern\"" }, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "Requires the pattern to match the entire string."
)

@SuppressWarnings("unused")
public class RelStringSplitActivity implements Activity, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        if ( type != null && settings.keysPresent( "column", "mode", "delimiter", "limit", "pattern", "keepCol", "prefix" ) ) {
            AlgDataType outType = getType( type,
                    settings.getString( "column" ),
                    settings.getString( "mode" ),
                    settings.getString( "delimiter" ),
                    settings.getInt( "limit" ),
                    settings.getString( "pattern" ),
                    settings.getBool( "keepCol" ),
                    settings.getString( "prefix" )
            );
            return RelType.of( outType ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType( inTypes.get( 0 ),
                settings.getString( "column" ),
                settings.getString( "mode" ),
                settings.getString( "delimiter" ),
                settings.getInt( "limit" ),
                settings.getString( "pattern" ),
                settings.getBool( "keepCol" ),
                settings.getString( "prefix" )
        );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        String column = settings.getString( "column" );
        int targetIdx = inputs.get( 0 ).getType().getFieldNames().indexOf( column );
        boolean keepCol = settings.getBool( "keepCol" );
        boolean fail = settings.getBool( "fail" );

        if ( settings.getString( "mode" ).equals( "delimiter" ) ) {
            delimiterPipe( inputs.get( 0 ), output, targetIdx, settings.getString( "delimiter" ), settings.getInt( "limit" ), keepCol, fail );
        } else {
            patternPipe( inputs.get( 0 ), output, targetIdx, settings.getString( "pattern" ), settings.getBool( "ignoreCase" ), settings.getBool( "matchAll" ), keepCol, fail );

        }
    }


    private void delimiterPipe( InputPipe input, OutputPipe output, int targetIdx, String delimiter, int limit, boolean keepCol, boolean fail ) throws Exception {
        for ( List<PolyValue> row : input ) {
            String value = row.get( targetIdx ).asString().value;
            List<PolyValue> splits = Arrays.stream( value.split( delimiter, limit ) ).map( s -> (PolyValue) PolyString.of( s ) ).toList();
            if ( splits.size() != limit ) {
                if ( fail ) {
                    throw new GenericRuntimeException( "Insufficient parts after splitting the string '" + value + "': " + splits.size() );
                }
                splits = fillList( splits, limit, PolyNull.NULL );
            }
            if ( !write( output, targetIdx, row, splits, keepCol ) ) {
                input.finishIteration();
                return;
            }
        }

    }


    private void patternPipe( InputPipe input, OutputPipe output, int targetIdx, String pattern, boolean ignoreCase, boolean matchAll, boolean keepCol, boolean fail ) throws Exception {
        Pattern p = ignoreCase ? Pattern.compile( pattern, Pattern.CASE_INSENSITIVE ) : Pattern.compile( pattern );
        int count = Pattern.compile( pattern ).matcher( "" ).groupCount();
        for ( List<PolyValue> row : input ) {
            String value = row.get( targetIdx ).asString().value;
            Matcher matcher = p.matcher( value );

            List<PolyValue> splits = new ArrayList<>();
            if ( matchAll ? matcher.matches() : matcher.find() ) {
                for ( int i = 1; i <= count; i++ ) {
                    splits.add( PolyString.of( matcher.group( i ) ) );
                }
            } else {
                if ( fail ) {
                    throw new GenericRuntimeException( "Unable to match pattern to: " + value );
                }
                splits = fillList( splits, count, PolyNull.NULL );
            }

            if ( !write( output, targetIdx, row, splits, keepCol ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private boolean write( OutputPipe output, int targetIdx, List<PolyValue> row, List<PolyValue> splits, boolean keepCol ) throws InterruptedException {
        List<PolyValue> outRow = new ArrayList<>( row );
        outRow.addAll( targetIdx + 1, splits );
        if ( !keepCol ) {
            outRow.remove( targetIdx );
        }
        return output.put( outRow );
    }


    private AlgDataType getType( AlgDataType type, String column, String mode, String delimiter, int limit, String pattern, boolean keepCol, String prefix ) throws InvalidSettingException {
        if ( !type.getFieldNames().contains( column ) ) {
            throw new InvalidSettingException( "Column " + column + " does not exist", "column" );
        }
        Builder builder = ActivityUtils.getBuilder();
        for ( AlgDataTypeField field : type.getFields() ) {
            if ( !field.getName().equals( column ) ) {
                builder.add( field );
                continue;
            }
            AlgDataType colType = field.getType();
            if ( colType.getPolyType().getFamily() != PolyTypeFamily.CHARACTER ) {
                throw new InvalidSettingException( "The target column must have a textual type, but found: " + colType.getPolyType(), "column" );
            }

            if ( keepCol ) {
                builder.add( field );
            }
            if ( prefix.isBlank() ) {
                prefix = field.getName();
            } else {
                if ( ActivityUtils.isInvalidFieldName( prefix + "0" ) ) {
                    throw new InvalidSettingException( "Invalid column prefix: " + prefix, "prefix" );
                }
            }

            int count;
            if ( mode.equals( "delimiter" ) ) {
                count = limit;
            } else {
                assert mode.equals( "pattern" );
                Pattern p = Pattern.compile( pattern );
                count = Pattern.compile( pattern ).matcher( "" ).groupCount();
                if ( count < 1 ) {
                    throw new InvalidSettingException( "The pattern must define at least one capturing group", "pattern" );
                }
            }
            for ( int i = 0; i < count; i++ ) {
                builder.add( prefix + i, null, colType ).nullable( true );
            }
        }
        return builder.uniquify().build();
    }


    public static List<PolyValue> fillList( List<PolyValue> splits, int count, PolyValue fillValue ) {
        List<PolyValue> result = new ArrayList<>( splits );
        while ( result.size() < count ) {
            result.add( fillValue );
        }
        return result;
    }

}
