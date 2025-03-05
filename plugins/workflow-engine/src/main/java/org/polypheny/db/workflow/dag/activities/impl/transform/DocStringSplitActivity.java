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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
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
@ActivityDefinition(type = "docStringSplit", displayName = "Split String Field", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.DOC, description = "A collection with a string field to be split") },
        outPorts = { @OutPort(type = PortType.DOC, description = "A collection where each document from the input collection has a new array field containing splits of the specified target field.") },
        shortDescription = "Split a string (sub)field into an array of strings."
)

@StringSetting(key = "field", displayName = "Field to Split", pos = 0,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, nonBlank = true, maxLength = 1024,
        shortDescription = "The string field to split. If the field does not exist or is not a string, the activity fails.")
@EnumSetting(key = "mode", displayName = "Splitting Mode", pos = 1,
        options = { "delimiter", "pattern" },
        displayOptions = { "Delimiter", "Match Pattern" },
        displayDescriptions = { "Specify a Regex delimiter and a limit.", "Match capturing groups specified using Regex." },
        defaultValue = "delimiter", style = EnumStyle.RADIO_BUTTON)

@StringSetting(key = "delimiter", displayName = "Regex Delimiter", nonBlank = true, pos = 10,
        defaultValue = ",",
        subPointer = "mode", subValues = { "\"delimiter\"" }, containsRegex = true, maxLength = 1024)
@IntSetting(key = "limit", displayName = "Maximum Number of Parts", pos = 11,
        subPointer = "mode", subValues = { "\"delimiter\"" }, defaultValue = 0, min = 0, max = 10000,
        shortDescription = "The maximum number of parts to create, or 0 to set no limit.")

@StringSetting(key = "pattern", displayName = "Regex Pattern", nonBlank = true, pos = 100,
        defaultValue = "https?://([^/]+)",
        subPointer = "mode", subValues = { "\"pattern\"" }, containsRegex = true, maxLength = 1024)
@BoolSetting(key = "ignoreCase", displayName = "Ignore Case", pos = 101,
        subPointer = "mode", subValues = { "\"pattern\"" }
)

@BoolSetting(key = "keepField", displayName = "Keep Original Field", pos = 0,
        group = GroupDef.ADVANCED_GROUP, defaultValue = false
)
@BoolSetting(key = "fail", displayName = "Fail on Invalid Split", pos = 1,
        group = GroupDef.ADVANCED_GROUP, defaultValue = false,
        shortDescription = "If true, the activity fails if a value cannot be split into enough parts or the pattern cannot be matched."
)
@BoolSetting(key = "fillMissing", displayName = "Insert Null for Missing Splits", pos = 2,
        group = GroupDef.ADVANCED_GROUP,
        subPointer = "fail", subValues = { "false" }, defaultValue = false,
        shortDescription = "If true, ensures that the array always has the same length, equal to the maximum number of expected splits. Has no effect if 'limit' is set to 0."
)
@StringSetting(key = "target", displayName = "Output Field", pos = 3,
        maxLength = 128, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "The output (sub)field for the splits array. If empty, the name is derived from the input field and inserted as a top level field.")
@BoolSetting(key = "matchAll", displayName = "Match Entire String", pos = 4,
        subPointer = "mode", subValues = { "\"pattern\"" }, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "Requires the pattern to match the entire string."
)

@SuppressWarnings("unused")
public class DocStringSplitActivity implements Activity, Pipeable {

    private static final String SPLIT_SUFFIX = "_split";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "mode", "pattern" ) ) {
            String pattern = settings.getString( "pattern" );
            if ( settings.getString( "mode" ).equals( "pattern" ) ) {
                Pattern p = Pattern.compile( pattern );
                int count = Pattern.compile( pattern ).matcher( "" ).groupCount();
                if ( count < 1 ) {
                    throw new InvalidSettingException( "The pattern must define at least one capturing group", "pattern" );
                }
            }
        }

        Set<String> fields = new HashSet<>();
        if ( inTypes.get( 0 ) instanceof DocType docType ) {
            fields.addAll( docType.getKnownFields() );
        }

        String field = settings.getNullableString( "field" );
        if ( field != null && settings.keysPresent( "target", "keepField" ) ) {
            if ( !settings.getBool( "keepField" ) ) {
                fields.remove( field );
            }
            String target = settings.getString( "target" );
            if ( target.isBlank() ) {
                fields.add( ActivityUtils.getChildPointer( field ) + SPLIT_SUFFIX );
            } else {
                String targetField = ActivityUtils.getChildPointer( target );
                if ( ActivityUtils.isInvalidFieldName( targetField ) ) {
                    throw new InvalidSettingException( "Invalid field name: " + targetField, "target" );
                }
                if ( !target.contains( "." ) ) {
                    fields.add( target );
                }
            }
        }

        return DocType.of( fields ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        String field = settings.getString( "field" );
        String target = settings.getString( "target" );
        boolean keepField = settings.getBool( "keepField" );
        boolean fail = settings.getBool( "fail" );
        boolean fillMissing = settings.getBool( "fillMissing" );
        if ( target.isBlank() ) {
            target = ActivityUtils.getChildPointer( field ) + SPLIT_SUFFIX;
        }

        if ( settings.getString( "mode" ).equals( "delimiter" ) ) {
            delimiterPipe( inputs.get( 0 ), output, field, target, settings.getString( "delimiter" ), settings.getInt( "limit" ), fillMissing, keepField, fail );
        } else {
            patternPipe( inputs.get( 0 ), output, field, target, settings.getString( "pattern" ), settings.getBool( "ignoreCase" ), settings.getBool( "matchAll" ), fillMissing, keepField, fail );

        }
    }


    private void delimiterPipe(
            InputPipe input, OutputPipe output, String field, String targetField, String delimiter,
            int limit, boolean fillMissing, boolean keepField, boolean fail ) throws Exception {
        boolean hasLimit = limit > 0;
        for ( List<PolyValue> row : input ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            String value = ActivityUtils.getSubValue( doc, field ).asString().value;
            List<PolyValue> splits = Arrays.stream( value.split( delimiter, limit ) ).map( s -> (PolyValue) PolyString.of( s ) ).toList();
            if ( hasLimit && splits.size() != limit ) {
                if ( fail ) {
                    throw new GenericRuntimeException( "Insufficient parts after splitting the string '" + value + "': " + splits.size() );
                }
                if ( fillMissing ) {
                    splits = RelStringSplitActivity.fillList( splits, limit, PolyNull.NULL );
                }
            }
            if ( !write( output, doc, field, targetField, splits, keepField ) ) {
                input.finishIteration();
                return;
            }
        }

    }


    private void patternPipe(
            InputPipe input, OutputPipe output, String field, String targetField, String pattern,
            boolean ignoreCase, boolean matchAll, boolean fillMissing, boolean keepField, boolean fail ) throws Exception {
        Pattern p = ignoreCase ? Pattern.compile( pattern, Pattern.CASE_INSENSITIVE ) : Pattern.compile( pattern );
        int count = Pattern.compile( pattern ).matcher( "" ).groupCount();
        for ( List<PolyValue> row : input ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            String value = ActivityUtils.getSubValue( doc, field ).asString().value;
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
                if ( fillMissing ) {
                    splits = RelStringSplitActivity.fillList( splits, count, PolyNull.NULL );
                }
            }

            if ( !write( output, doc, field, targetField, splits, keepField ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private boolean write( OutputPipe output, PolyDocument doc, String field, String targetField, List<PolyValue> splits, boolean keepField ) throws Exception {
        if ( !keepField ) {
            ActivityUtils.removeSubValue( doc, field );
        }
        ActivityUtils.insertSubValue( doc, targetField, PolyList.of( splits ) );
        return output.put( doc );
    }

}
