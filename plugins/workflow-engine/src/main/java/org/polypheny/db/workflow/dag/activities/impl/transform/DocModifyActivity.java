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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
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
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docModify", displayName = "Modify Document Structure", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection of documents.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The output collection with modified documents.") },
        shortDescription = "Modifies the structure of each input document."
)

@EnumSetting(key = "mode", displayName = "Modification", pos = 0,
        options = { "move", "flatten", "remove", "arrWrap", "parse", "emptyObj", "emptyArr", "null", "setValue" },
        displayOptions = { "Move", "Flatten", "Remove", "Wrap in Array", "Parse JSON", "Insert Empty Object", "Insert Empty Array", "Insert Null", "Set to JSON Value" },
        defaultValue = "move",
        shortDescription = "The type of modification to apply to each document.")
@StringSetting(key = "source", displayName = "Source Field", pos = 1,
        autoCompleteType = AutoCompleteType.FIELD_NAMES,
        shortDescription = "The (sub)field to modify.")
@StringSetting(key = "target", displayName = "Target Field / Value", pos = 2,
        subPointer = "mode", subValues = { "move", "setValue" },
        shortDescription = "The target (sub)field or value of the operation.")
@BoolSetting(key = "fail", displayName = "Fail on Invalid Field", pos = 3, defaultValue = true,
        shortDescription = "If false, a failed modification results in the document to be skipped instead of failing the execution.")
@SuppressWarnings("unused")
public class DocModifyActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof DocType docType && settings.keysPresent( "mode", "source", "target" ) ) {
            String mode = settings.getString( "mode" );
            String source = settings.getString( "source" );
            String target = settings.getString( "target" );
            boolean topLvlSource = !source.isBlank() && !source.contains( "." );
            boolean topLvlTarget = !target.isBlank() && !target.contains( "." );
            Set<String> fields = new HashSet<>( docType.getKnownFields() );
            switch ( mode ) {
                case "move" -> {
                    fields.remove( target );
                    if ( target.isBlank() ) {
                        throw new InvalidSettingException( "Target must not be empty", "target" );
                    }
                    String child = ActivityUtils.getChildPointer( target );
                    if ( !child.matches( "\\d+" ) && ActivityUtils.isInvalidFieldName( child ) ) {
                        throw new InvalidSettingException( "Invalid target field: " + target, "target" );
                    }
                    if ( topLvlTarget ) {
                        fields.add( target );
                    }
                }
                case "flatten", "remove" -> fields.remove( source );
                case "emptyObj", "emptyArr", "null" -> {
                    if ( topLvlSource ) {
                        fields.add( source );
                    }
                }
                case "setValue" -> {
                    if ( source.isBlank() ) {
                        throw new InvalidSettingException( "Source must not be empty", "source" );
                    }
                    String child = ActivityUtils.getChildPointer( source );
                    if ( !child.matches( "\\d+" ) && ActivityUtils.isInvalidFieldName( child ) ) {
                        throw new InvalidSettingException( "Invalid field name: " + source, "source" );
                    }
                    if ( topLvlSource ) {
                        fields.add( source );
                    }
                }
            }
            return DocType.of( fields ).asOutTypes();
        }
        return DocType.of().asOutTypes();
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
        String mode = settings.getString( "mode" );
        String source = settings.getString( "source" );
        String target = settings.getString( "target" );
        boolean fail = settings.getBool( "fail" );
        long skipCount = 0;
        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            PolyDocument doc;
            try {
                doc = modify( value.get( 0 ).asDocument(), mode, source, target );
            } catch ( Exception e ) {
                if ( fail ) {
                    if ( e instanceof NumberFormatException nfe ) {
                        throw new IllegalArgumentException( "Invalid number format: " + nfe.getMessage() );
                    }
                    throw e;
                }
                skipCount++;
                continue; // skip failed document
            }
            if ( !output.put( doc ) ) {
                finish( inputs );
                break;
            }
        }
        if ( skipCount > 0 ) {
            ctx.logWarning( "Skipped " + skipCount + " documents that could not be modified successfully." );
        }
    }


    public static PolyDocument modify( PolyDocument doc, String mode, String source, String target ) throws Exception {
        switch ( mode ) {
            case "move" -> {
                notBlank( source );
                PolyValue value = ActivityUtils.removeSubValue( doc, source );
                if ( value == null ) {
                    throw new IllegalArgumentException( "Source field does not exist: " + source );
                }
                ActivityUtils.insertSubValue( doc, target, value );
                String parentPointer = ActivityUtils.getParentPointer( source );
                PolyValue parent = ActivityUtils.getSubValue( doc, parentPointer );
                if ( (parent.isDocument() && parent.asDocument().isEmpty()) ||
                        (parent.isList() && parent.asList().isEmpty()) ) {
                    ActivityUtils.removeSubValue( doc, parentPointer );
                }
            }
            case "flatten" -> {
                notBlank( source );
                PolyValue value = ActivityUtils.removeSubValue( doc, source ); // empty array are removed completely
                String parentPointer = ActivityUtils.getParentPointer( source );
                PolyValue parent = ActivityUtils.getSubValue( doc, parentPointer );
                Map<PolyString, PolyValue> values;
                if ( value.isDocument() ) {
                    values = value.asDocument();
                } else if ( value instanceof PolyList<?> list ) {
                    values = new LinkedHashMap<>();
                    if ( list.size() == 1 ) {
                        ActivityUtils.insertSubValue( doc, source, list.get( 0 ) ); // unwrap single element directly
                        break;
                    }
                    for ( int i = 0; i < list.size(); i++ ) {
                        values.put( PolyString.of( String.valueOf( i ) ), list.get( i ) );
                    }
                } else {
                    throw new IllegalArgumentException( "Invalid value type at source: " + source );
                }
                if ( parent.isDocument() ) {
                    parent.asDocument().putAll( values );
                } else {
                    parent.asList().addAll( values.values() );
                }
            }
            case "remove" -> ActivityUtils.removeSubValue( doc, source );
            case "arrWrap" -> {
                notBlank( source );
                PolyValue value = ActivityUtils.removeSubValue( doc, source );
                ActivityUtils.insertSubValue( doc, source, PolyList.of( value ) );
            }
            case "parse" -> {
                notBlank( source );
                PolyValue value = ActivityUtils.removeSubValue( doc, source );
                if ( value.isString() ) {
                    ActivityUtils.insertSubValue( doc, source, PolyValue.fromJson( value.asString().value ) );
                } else {
                    throw new IllegalArgumentException( "Source field does not represent a string: " + source );
                }
            }
            case "emptyObj" -> {
                notBlank( source );
                ActivityUtils.insertSubValue( doc, source, new PolyDocument() );
            }
            case "emptyArr" -> {
                notBlank( source );
                ActivityUtils.insertSubValue( doc, source, PolyList.of() );
            }
            case "null" -> {
                notBlank( source );
                ActivityUtils.insertSubValue( doc, source, PolyNull.NULL );
            }
            case "setValue" -> {
                notBlank( source );
                PolyValue value;
                try {
                    value = PolyValue.fromJson( target );
                } catch ( Exception e ) {
                    value = PolyString.of( target );
                }
                ActivityUtils.insertSubValue( doc, source, value );
            }
        }
        return doc;
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        String mode = settings.getNullableString( "mode" );
        if ( mode != null ) {
            return "Modify Document: " + modeToString( mode );
        }
        return null;
    }


    public static String modeToString( String mode ) {
        return switch ( mode ) {
            case "move" -> "Move";
            case "flatten" -> "Flatten";
            case "remove" -> "Remove";
            case "arrWrap" -> "Wrap in Array";
            case "parse" -> "Parse JSON";
            case "emptyObj" -> "Insert Empty Object";
            case "emptyArr" -> "Insert Empty Array";
            case "null" -> "Insert Null";
            case "setValue" -> "Set Value";
            default -> throw new IllegalStateException( "Unexpected value: " + mode );
        };
    }


    private static void notBlank( String s ) throws IllegalArgumentException {
        if ( s.isBlank() ) {
            throw new IllegalArgumentException( "Value cannot be blank." );
        }
    }

}
