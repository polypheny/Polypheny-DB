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

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "lpgStringClean", displayName = "Clean String Properties", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.LPG) },
        outPorts = { @OutPort(type = PortType.LPG, description = "A graph with the specified string properties of nodes / edges cleaned.") },
        shortDescription = "Cleans the selected string properties of graph elements according to the specified settings."
)

@FieldSelectSetting(key = "targets", displayName = "Target String Field(s)", simplified = true, pos = 0,
        forLabels = false, targetInput = 0,
        shortDescription = "The string (sub)fields to clean.")
@EnumSetting(key = "trim", displayName = "Trim Whitespaces", pos = 1,
        options = { "none", "leading", "trailing", "both" }, defaultValue = "none",
        displayOptions = { "None", "Leading", "Trailing", "Both" }, style = EnumStyle.RADIO_BUTTON)
@EnumSetting(key = "casing", displayName = "Change Casing", pos = 2,
        options = { "none", "upper", "lower" }, defaultValue = "none",
        displayOptions = { "None", "To Upper Case", "To Lower Case" },
        style = EnumStyle.RADIO_BUTTON)
@BoolSetting(key = "isReplace", displayName = "Search / Replace", pos = 3,
        defaultValue = false,
        shortDescription = "If true, enables custom removing or replacing of characters that are matched to a Regex. It is applied before any other cleaning is performed.")
@StringSetting(key = "search", displayName = "Search Regex", pos = 4,
        defaultValue = "[^a-zA-Z0-9]",
        subPointer = "isReplace", subValues = { "true" }, containsRegex = true, maxLength = 1024)
@StringSetting(key = "replace", displayName = "Replacement", pos = 5,
        defaultValue = "",
        subPointer = "isReplace", subValues = { "true" }, maxLength = 1024,
        shortDescription = "The replacement for all matches. Can be left empty to remove all matches."
                + " The replacement can reference capture groups such as '$0' for the original match.")
@BoolSetting(key = "ignoreCase", displayName = "Case Insensitive", pos = 6,
        defaultValue = false, subPointer = "isReplace", subValues = { "true" },
        shortDescription = "If true, the search Regex is matched in a case insensitive manner.")

@FieldSelectSetting(key = "labels", displayName = "Target Labels", simplified = true, pos = 7,
        targetInput = 0, forLabels = true, group = ADVANCED_GROUP,
        shortDescription = "Specify the target nodes or edges by their label(s). If no label is specified, all become targets.")
@BoolSetting(key = "nodes", displayName = "Clean Node Properties", defaultValue = true, pos = 8, group = ADVANCED_GROUP)
@BoolSetting(key = "edges", displayName = "Clean Edge Properties", defaultValue = true, pos = 9, group = ADVANCED_GROUP)

@SuppressWarnings("unused")
public class LpgStringCleanActivity implements Activity, Pipeable {

    private List<PolyString> targets;
    private boolean isReplace;
    private String search;
    private String replace;
    private boolean ignoreCase;

    private String trim;
    private String casing;
    private Pattern pattern;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return inTypes.get( 0 ).asOutTypes();
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
        targets = settings.get( "targets", FieldSelectValue.class ).getInclude().stream().map( PolyString::of ).toList();
        isReplace = settings.getBool( "isReplace" );
        search = settings.getString( "search" );
        replace = settings.getString( "replace" );
        ignoreCase = settings.getBool( "ignoreCase" );

        trim = settings.getString( "trim" );
        casing = settings.getString( "casing" );
        pattern = isReplace ? Pattern.compile( search, ignoreCase ? Pattern.CASE_INSENSITIVE : 0 ) : null;

        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean isNodes = settings.getBool( "nodes" );
        boolean isEdges = settings.getBool( "edges" );

        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        for ( PolyNode node : input.getNodeIterable() ) {
            if ( isNodes && ActivityUtils.matchesLabelList( node, labels ) ) {
                cleanProperties( node.properties );
            }

            if ( !output.put( node ) ) {
                finish( inputs );
                return;
            }
        }

        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( isEdges && ActivityUtils.matchesLabelList( edge, labels ) ) {
                cleanProperties( edge.properties );
            }

            if ( !output.put( edge ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private void cleanProperties( PolyDictionary props ) {
        for ( PolyString target : targets ) {
            String s;
            try {
                PolyValue value = props.get( target );
                if ( value == null || value.isNull() ) {
                    continue;
                }
                s = value.asString().value;
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Value at '" + target + "' is not a string for properties: " + props );
            }
            if ( isReplace ) {
                s = pattern.matcher( s ).replaceAll( replace );
            }
            switch ( trim ) {
                case "leading" -> s = s.stripLeading();
                case "trailing" -> s = s.stripTrailing();
                case "both" -> s = s.strip();
            }
            switch ( casing ) {
                case "upper" -> s = s.toUpperCase( Locale.ROOT );
                case "lower" -> s = s.toLowerCase( Locale.ROOT );
            }
            props.put( target, PolyString.of( s ) );
        }
    }

}
