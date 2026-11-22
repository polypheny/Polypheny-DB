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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.fun.TrimFunction.Flag;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relStringClean", displayName = "Clean String Columns", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL, description = "A table with the specified string columns cleaned.") },
        shortDescription = "Cleans the selected string columns according to the specified settings. Might change the type of VARCHAR columns to accommodate longer strings."
)

@FieldSelectSetting(key = "targets", displayName = "Target String Column(s)", simplified = true, targetInput = 0, pos = 0,
        shortDescription = "The string columns to clean.")
@EnumSetting(key = "trim", displayName = "Trim Whitespaces", pos = 1,
        options = { "none", "leading", "trailing", "both" }, defaultValue = "none",
        displayOptions = { "None", "Leading", "Trailing", "Both" }, style = EnumStyle.RADIO_BUTTON,
        shortDescription = "Note that if this activity gets fused, only spaces are trimmed. Otherwise, all unicode whitespaces are considered.")
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

@SuppressWarnings("unused")
public class RelStringCleanActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        if ( type != null && settings.keysPresent( "targets", "isReplace", "search", "replace" ) ) {
            List<String> targets = settings.getOrThrow( "targets", FieldSelectValue.class ).getInclude();
            if ( targets.isEmpty() ) {
                throw new InvalidSettingException( "At least one column must be selected", "targets" );
            }
            for ( AlgDataTypeField field : type.getFields() ) {
                PolyType polyType = field.getType().getPolyType();
                if ( targets.contains( field.getName() ) && polyType.getFamily() != PolyTypeFamily.CHARACTER ) {
                    throw new InvalidSettingException( "Target column is not textual: " + field.getName(), "targets" );
                }
            }

            boolean isReplace = settings.getBool( "isReplace" );
            String search = settings.getString( "search" );
            if ( isReplace && search.isEmpty() ) {
                throw new InvalidSettingException( "Search expression must not be empty", "search" );
            }
            String replace = settings.getString( "replace" );
            return RelType.of( getType( type, targets, isReplace, search, replace ) ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        List<String> targets = settings.get( "targets", FieldSelectValue.class ).getInclude();
        boolean isReplace = settings.getBool( "isReplace" );
        String search = settings.getString( "search" );
        String replace = settings.getString( "replace" );
        return getType( inTypes.get( 0 ), targets, isReplace, search, replace );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        Set<String> targets = new HashSet<>( settings.get( "targets", FieldSelectValue.class ).getInclude() );
        boolean isReplace = settings.getBool( "isReplace" );
        String search = settings.getString( "search" );
        String replace = settings.getString( "replace" );
        boolean ignoreCase = settings.getBool( "ignoreCase" );

        String trim = settings.getString( "trim" );
        String casing = settings.getString( "casing" );

        List<String> names = output.getType().getFieldNames();
        Pattern pattern = isReplace ? Pattern.compile( search, ignoreCase ? Pattern.CASE_INSENSITIVE : 0 ) : null;

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            List<PolyValue> cleaned = new ArrayList<>();
            for ( int i = 0; i < row.size(); i++ ) {
                String name = names.get( i );
                PolyValue value = row.get( i );
                if ( targets.contains( name ) && !value.isNull() ) {
                    String s = value.asString().value;
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
                    cleaned.add( PolyString.of( s ) );
                } else {
                    cleaned.add( value );
                }
            }

            if ( !output.put( cleaned ) ) {
                finish( inputs );
                return;
            }
        }


    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx ); // Pipe is easier to control than fuse
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "isReplace", BoolValue.class ).map( b -> !b.getValue() );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        assert !settings.getBool( "isReplace" );
        Set<String> targets = new HashSet<>( settings.get( "targets", FieldSelectValue.class ).getInclude() );
        String trim = settings.getString( "trim" );
        String casing = settings.getString( "casing" );

        AlgDataType inType = inputs.get( 0 ).getTupleType();
        RexBuilder builder = cluster.getRexBuilder();
        List<RexNode> projects = new ArrayList<>();

        RexLiteral space = ActivityUtils.getRexLiteral( " " );
        RexLiteral flag = switch ( trim ) {
            case "leading" -> builder.makeFlag( Flag.LEADING );
            case "trailing" -> builder.makeFlag( Flag.TRAILING );
            case "both" -> builder.makeFlag( Flag.BOTH );
            default -> null;
        };
        for ( int i = 0; i < inType.getFieldCount(); i++ ) {
            AlgDataTypeField field = inType.getFields().get( i );
            RexNode rex = new RexIndexRef( i, field.getType() );
            if ( targets.contains( field.getName() ) ) {
                if ( flag != null ) {
                    rex = builder.makeCall( OperatorRegistry.get( OperatorName.TRIM ), flag, space, rex );
                }

                switch ( casing ) {
                    case "upper" -> rex = builder.makeCall( OperatorRegistry.get( OperatorName.UPPER ), rex );
                    case "lower" -> rex = builder.makeCall( OperatorRegistry.get( OperatorName.LOWER ), rex );
                }
            }
            projects.add( rex );
        }

        return LogicalRelProject.create( inputs.get( 0 ), projects, inType );
    }


    private AlgDataType getType( AlgDataType inType, List<String> targets, boolean isReplace, String search, String replace ) {
        if ( !isReplace || replace.length() <= 1 ) { // string length won't get longer
            return inType;
        }

        Builder builder = ActivityUtils.getBuilder();
        for ( AlgDataTypeField field : inType.getFields() ) {
            if ( targets.contains( field.getName() ) ) {
                builder.add( field.getName(), null, PolyType.TEXT ); // arbitrary length string to be sure
            } else {
                builder.add( field );
            }
        }
        return builder.build();
    }

}
