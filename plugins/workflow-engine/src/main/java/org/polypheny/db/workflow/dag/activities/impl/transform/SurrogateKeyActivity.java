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
import java.util.Set;
import java.util.UUID;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "surrogateKey", displayName = "Add Surrogate Key", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.ANY, description = "The input table, collection or graph.") },
        outPorts = { @OutPort(type = PortType.ANY, description = "The input with an added surrogate key to each row, document or node.") },
        shortDescription = "Adds a generated surrogate key to each row, document or node in the input."
)

@StringSetting(key = "name", displayName = "Field Name", pos = 0,
        nonBlank = true, defaultValue = "sk",
        shortDescription = "Specify the name for the key field.")
@EnumSetting(key = "generator", displayName = "Key Generation", style = EnumStyle.RADIO_BUTTON, pos = 1,
        options = { "auto", "uuid" }, defaultValue = "auto",
        displayOptions = { "Auto-Increment", "UUID" },
        shortDescription = "Define the key generation method.")
@IntSetting(key = "startKey", displayName = "First Key", min = 0, defaultValue = 1,
        subPointer = "generator", subValues = { "\"auto\"" },
        shortDescription = "The value of the first surrogate key when using the auto-increment key generation.")
@SuppressWarnings("unused")
public class SurrogateKeyActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview type = inTypes.get( 0 );
        if ( !type.isPresent() ) {
            return UnknownType.of().asOutTypes();
        }

        String name = settings.getNullableString( "name" );
        String generator = settings.getNullableString( "generator" );

        if ( name != null ) {
            if ( type instanceof RelType relType ) {
                if ( generator != null ) {
                    return RelType.of( getRelOutType( relType.getNullableType(), name, generator ) ).asOutTypes();
                }
                return UnknownType.ofRel().asOutTypes();
            } else if ( type instanceof DocType docType ) {
                Set<String> fields = new HashSet<>( docType.getKnownFields() );
                fields.add( name );
                return DocType.of( fields ).asOutTypes();
            }
        }
        return type.asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        AlgDataType type = inTypes.get( 0 );
        return switch ( ActivityUtils.getDataModel( type ) ) {
            case RELATIONAL -> getRelOutType( type, settings.getString( "name" ), settings.getString( "generator" ) );
            case DOCUMENT -> getDocType();
            case GRAPH -> getGraphType();
        };
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        InputPipe input = inputs.get( 0 );
        String name = settings.getString( "name" );
        String generator = settings.getString( "generator" );
        int startKey = settings.getInt( "startKey" );
        switch ( ActivityUtils.getDataModel( input.getType() ) ) {
            case RELATIONAL -> relPipe( input, output, generator, startKey, ctx );
            case DOCUMENT -> docPipe( input, output, name, generator, startKey, ctx );
            case GRAPH -> lpgPipe( input.asLpgInputPipe(), output, name, generator, startKey, ctx );
        }
    }


    private void relPipe( InputPipe input, OutputPipe output, String generator, int startKey, PipeExecutionContext ctx ) throws Exception {
        long key = startKey;
        for ( List<PolyValue> value : input ) {
            List<PolyValue> row = new ArrayList<>( value );
            switch ( generator ) {
                case "auto" -> row.add( 1, PolyLong.of( key++ ) );
                case "uuid" -> row.add( 1, PolyString.of( UUID.randomUUID().toString() ) );
            }
            if ( !output.put( row ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private void docPipe( InputPipe input, OutputPipe output, String name, String generator, int startKey, PipeExecutionContext ctx ) throws Exception {
        PolyString nameStr = PolyString.of( name );
        long key = startKey;
        for ( List<PolyValue> value : input ) {
            PolyDocument doc = value.get( 0 ).asDocument();
            switch ( generator ) {
                case "auto" -> doc.put( nameStr, PolyLong.of( key++ ) );
                case "uuid" -> doc.put( nameStr, PolyString.of( UUID.randomUUID().toString() ) );
            }
            if ( !output.put( doc ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private void lpgPipe( LpgInputPipe input, OutputPipe output, String name, String generator, int startKey, PipeExecutionContext ctx ) throws Exception {
        PolyString nameStr = PolyString.of( name );
        long key = startKey;
        for ( PolyNode node : input.getNodeIterable() ) {
            switch ( generator ) {
                case "auto" -> node.properties.put( nameStr, PolyLong.of( key++ ) );
                case "uuid" -> node.properties.put( nameStr, PolyString.of( UUID.randomUUID().toString() ) );
            }
            if ( !output.put( node ) ) {
                input.finishIteration();
                return;
            }
        }
        for ( PolyEdge edge : input.getEdgeIterable() ) {
            if ( !output.put( edge ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private AlgDataType getRelOutType( AlgDataType inType, String name, String generator ) throws InvalidSettingException {
        if ( inType.getFieldNames().contains( name ) ) {
            throw new InvalidSettingException( "Column already exists: " + name, "name" );
        }
        if ( ActivityUtils.isInvalidFieldName( name ) ) {
            throw new InvalidSettingException( "Invalid column name: " + name, "name" );
        }
        List<AlgDataTypeField> fields = inType.getFields();
        Builder builder = ActivityUtils.getBuilder();
        builder.add( fields.get( 0 ) ); // Primary key
        switch ( generator ) {
            case "auto" -> {
                builder.add( name, null, PolyType.BIGINT );
            }
            case "uuid" -> {
                builder.add( name, null, PolyType.VARCHAR, 36 );
            }
            default -> throw new GenericRuntimeException( "Unknown generator: " + generator );
        }
        if ( fields.size() > 1 ) {
            builder.addAll( fields.subList( 1, fields.size() ) );
        }
        return builder.build();
    }

}
