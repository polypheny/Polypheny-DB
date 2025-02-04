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

package org.polypheny.db.workflow.dag.activities.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyLong;
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
import org.polypheny.db.workflow.dag.annotations.CastSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.settings.CastValue;
import org.polypheny.db.workflow.dag.settings.CastValue.SingleCast;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docToRel", displayName = "Collection to Table", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection.") },
        outPorts = { @OutPort(type = PortType.REL, description = "A table containing the input documents as rows.") },
        shortDescription = "Defines a mapping from documents in a collection to rows in a table."
)
@DefaultGroup(subgroups = { @Subgroup(key = "manual", displayName = "Manual Mapping") })

@BoolSetting(key = "trivial", displayName = "Automatic Mapping", defaultValue = false, pos = 0,
        shortDescription = "Map each document to a column containing its id and a column containing its JSON representation.")
@CastSetting(key = "columns", displayName = "Fields to Columns", defaultType = PolyType.BIGINT, pos = 1, subGroup = "manual",
        allowTarget = true, duplicateSource = true, allowJson = true, subPointer = "trivial", subValues = { "false" },
        shortDescription = "Specify how (sub)fields are mapped to columns.")
@EnumSetting(key = "handleMissing", displayName = "Missing Field Handling", options = { "null", "skip", "fail" }, defaultValue = "fail",
        pos = 2, subGroup = "manual",
        displayOptions = { "Use Null", "Skip Document", "Fail Execution" }, subPointer = "trivial", subValues = { "false" },
        shortDescription = "Determines the strategy for handling documents where a specified field is missing.")
@BoolSetting(key = "unspecified", displayName = "Column for Unspecified Fields", defaultValue = false, pos = 3, subGroup = "manual",
        subPointer = "trivial", subValues = { "false" },
        shortDescription = "Adds a column containing any remaining unspecified (top-level) fields as JSON.")
@SuppressWarnings("unused")
public class DocToRelActivity implements Activity, Pipeable {

    private static final String UNSPECIFIED = "_other";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.allPresent() ) {
            AlgDataType type = getType(
                    settings.getBool( "trivial" ),
                    settings.getOrThrow( "columns", CastValue.class ),
                    settings.getBool( "unspecified" )
            );
            return RelType.of( type ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType(
                settings.getBool( "trivial" ),
                settings.get( "columns", CastValue.class ),
                settings.getBool( "unspecified" )
        );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        ctx.logInfo( "Output type: " + output.getType() );
        if ( settings.getBool( "trivial" ) ) {
            pipe( inputs.get( 0 ), output, ctx );
        } else {
            pipe( inputs.get( 0 ),
                    output,
                    settings.get( "columns", CastValue.class ),
                    settings.getBool( "unspecified" ),
                    settings.getString( "handleMissing" ),
                    ctx
            );
        }
    }


    private void pipe( InputPipe input, OutputPipe output, PipeExecutionContext ctx ) throws Exception {
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key
        for ( List<PolyValue> tuple : input ) {
            PolyDocument doc = tuple.get( 0 ).asDocument();
            // TODO: decide whether to remove _id from data
            //Map<PolyString, PolyValue> map = new HashMap<>(doc.map);
            //map.remove( _id );
            List<PolyValue> row = List.of(
                    pkVal,
                    doc.get( docId ),
                    doc.toPolyJson()
            );

            if ( !output.put( row ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private void pipe( InputPipe input, OutputPipe output, CastValue cols, boolean includeUnspecified, String missingStrategy, PipeExecutionContext ctx ) throws Exception {
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key

        Set<String> fields = cols.getCasts().stream().map( c -> c.getSource().split( "\\." )[0] ).collect( Collectors.toSet() );

        outer:
        for ( List<PolyValue> tuple : input ) {
            PolyDocument doc = tuple.get( 0 ).asDocument();
            List<PolyValue> row = new ArrayList<>();
            row.add( pkVal );

            for ( SingleCast col : cols.getCasts() ) {
                PolyValue value;
                try {
                    value = ActivityUtils.getSubValue( doc, col.getSource() );
                } catch ( Exception e ) {
                    switch ( missingStrategy ) {
                        case "null" -> {
                            value = col.getNullValue();
                        }
                        case "skip" -> {
                            continue outer;
                        }
                        case "fail" -> throw new GenericRuntimeException( "Missing field '" + col.getSource() +
                                "' for document '" + doc.toJson() + "'" );
                        default -> throw new IllegalStateException( "ignored" );
                    }
                }
                row.add( col.castValue( value ) );
            }

            if ( includeUnspecified ) {
                Map<PolyString, PolyValue> unspecified = new HashMap<>();
                for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
                    String key = entry.getKey().value;
                    if ( !key.equals( DocumentType.DOCUMENT_ID ) && !fields.contains( key ) ) {
                        unspecified.put( entry.getKey(), entry.getValue() );
                    }
                }
                if ( unspecified.isEmpty() ) {
                    row.add( PolyString.of( null ) );
                } else {
                    row.add( PolyDocument.ofDocument( unspecified ).toPolyJson() );
                }

            }

            if ( !output.put( row ) ) {
                input.finishIteration();
                return;
            }
        }

    }


    private AlgDataType getType( boolean isTrivial, CastValue cols, boolean includeUnspecified ) throws InvalidSettingException {
        if ( isTrivial ) {
            return ActivityUtils.getBuilder()
                    .add( StorageManager.PK_COL, null, factory.createPolyType( PolyType.BIGINT ) )
                    .add( DocumentType.DOCUMENT_ID, null, factory.createPolyType( PolyType.VARCHAR, 24 ) )
                    .add( DocumentType.DOCUMENT_DATA, null, PolyType.TEXT )
                    .build();
        }

        AlgDataType type = ActivityUtils.addPkCol( cols.asAlgDataType() );
        if ( includeUnspecified ) {
            type = ActivityUtils.appendField( type, UNSPECIFIED, factory.createPolyType( PolyType.TEXT ) );
        }
        Optional<String> invalid = ActivityUtils.findInvalidFieldName( type.getFieldNames() );
        if ( invalid.isPresent() ) {
            throw new InvalidSettingException( "Invalid column name: " + invalid.get(), "columns" );
        }
        return type;
    }

}
