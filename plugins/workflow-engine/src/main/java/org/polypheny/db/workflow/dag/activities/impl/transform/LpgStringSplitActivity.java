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
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "lpgStringSplit", displayName = "Split String Property", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.LPG, description = "A graph with a string property to be split") },
        outPorts = { @OutPort(type = PortType.LPG, description = "The graph where each target node or edge has a new array property containing splits of the specified target property.") },
        shortDescription = "Split a string property into an array of strings."
)

@StringSetting(key = "field", displayName = "Property to Split", pos = 0,
        nonBlank = true, maxLength = 1024,
        shortDescription = "The string property to split. If the property does not exist or is not a string, the activity fails.")
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

@FieldSelectSetting(key = "labels", displayName = "Target Labels", simplified = true, targetInput = 0, pos = 0,
        group = GroupDef.ADVANCED_GROUP,
        shortDescription = "Specify the target nodes or edges by their label(s). If no label is specified, all become targets.")
@BoolSetting(key = "keepField", displayName = "Keep Original Property", pos = 1,
        group = GroupDef.ADVANCED_GROUP, defaultValue = false
)
@BoolSetting(key = "fail", displayName = "Fail on Invalid Split", pos = 2,
        group = GroupDef.ADVANCED_GROUP, defaultValue = false,
        shortDescription = "If true, the activity fails if a value cannot be split into enough parts or the pattern cannot be matched."
)
@BoolSetting(key = "fillMissing", displayName = "Insert Null for Missing Splits", pos = 3,
        group = GroupDef.ADVANCED_GROUP,
        subPointer = "fail", subValues = { "false" }, defaultValue = false,
        shortDescription = "If true, ensures that the array always has the same length, equal to the maximum number of expected splits. Has no effect if 'limit' is set to 0."
)
@StringSetting(key = "target", displayName = "Output Field", pos = 4,
        maxLength = 128, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "The output property for the splits array. If empty, the name is derived from the input property.")
@BoolSetting(key = "matchAll", displayName = "Match Entire String", pos = 5,
        subPointer = "mode", subValues = { "\"pattern\"" }, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "Requires the pattern to match the entire string."
)

@SuppressWarnings("unused")
public class LpgStringSplitActivity implements Activity, Pipeable {

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

        String field = settings.getNullableString( "field" );
        if ( field != null && settings.keysPresent( "target" ) ) {
            String target = settings.getString( "target" );
            if ( !target.isBlank() ) {
                if ( ActivityUtils.isInvalidFieldName( target ) ) {
                    throw new InvalidSettingException( "Invalid property name: " + target, "target" );
                }
            }
        }

        if ( inTypes.get( 0 ) instanceof LpgType type ) {
            return type.asOutTypes();
        }
        return LpgType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        PolyString field = PolyString.of( settings.getString( "field" ) );
        PolyString target = PolyString.of( settings.getString( "target" ) );
        boolean isDelimiter = settings.getString( "mode" ).equals( "delimiter" );
        boolean keepField = settings.getBool( "keepField" );
        boolean fail = settings.getBool( "fail" );
        boolean fillMissing = settings.getBool( "fillMissing" );
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        if ( target.value.isBlank() ) {
            target = PolyString.of( field.value + SPLIT_SUFFIX );
        }

        String delimiter = settings.getString( "delimiter" );
        Pattern pattern = null;

        if ( !isDelimiter ) {
            String str = settings.getString( "pattern" );
            pattern = settings.getBool( "ignoreCase" ) ? Pattern.compile( str, Pattern.CASE_INSENSITIVE ) : Pattern.compile( str );
        }
        int limit = isDelimiter ? settings.getInt( "limit" ) : pattern.matcher( "" ).groupCount();
        boolean matchAll = settings.getBool( "matchAll" );

        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        for ( PolyNode node : input.getNodeIterable() ) {
            if ( ActivityUtils.matchesLabelList( node, labels ) ) {
                if ( isDelimiter ) {
                    delimiterSplit( node.properties, field, target, delimiter, limit, fillMissing, keepField, fail );
                } else {
                    patternSplit( node.properties, field, target, pattern, limit, matchAll, fillMissing, keepField, fail );
                }
            }
            if ( !output.put( node ) ) {
                finish( inputs );
                return;
            }
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( ActivityUtils.matchesLabelList( edge, labels ) ) {
                if ( isDelimiter ) {
                    delimiterSplit( edge.properties, field, target, delimiter, limit, fillMissing, keepField, fail );
                } else {
                    patternSplit( edge.properties, field, target, pattern, limit, matchAll, fillMissing, keepField, fail );
                }
            }
            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private void delimiterSplit( PolyDictionary dict, PolyString field, PolyString target, String delimiter, int limit, boolean fillMissing, boolean keepField, boolean fail ) {
        String value = dict.get( field ).asString().value;
        List<PolyValue> splits = Arrays.stream( value.split( delimiter, limit ) ).map( s -> (PolyValue) PolyString.of( s ) ).toList();
        if ( limit > 0 && splits.size() != limit ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Insufficient parts after splitting the string '" + value + "': " + splits.size() );
            }
            if ( fillMissing ) {
                splits = RelStringSplitActivity.fillList( splits, limit, PolyNull.NULL );
            }
        }
        if ( !keepField ) {
            dict.remove( field );
        }
        dict.put( target, PolyList.of( splits ) );
    }


    private void patternSplit(
            PolyDictionary dict, PolyString field, PolyString target, Pattern p, int count,
            boolean matchAll, boolean fillMissing, boolean keepField, boolean fail ) {
        String value = dict.get( field ).asString().value;
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

        if ( !keepField ) {
            dict.remove( field );
        }
        dict.put( target, PolyList.of( splits ) );
    }

}
