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

package org.polypheny.db.workflow.dag.activities.impl.crossmodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
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
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "docToRel", displayName = "Collection to Table", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.RELATIONAL, ActivityCategory.CROSS_MODEL, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection.") },
        outPorts = { @OutPort(type = PortType.REL, description = "A table containing the input documents as rows.") },
        shortDescription = "Defines a mapping from documents in a collection to rows in a table."
)
@DefaultGroup(subgroups = { @Subgroup(key = "manual", displayName = "Custom Mapping") })

@EnumSetting(key = "mode", displayName = "Schema Mapping", style = EnumStyle.RADIO_BUTTON, pos = 0,
        options = { "auto", "fixed", "manual" }, defaultValue = "auto",
        displayOptions = { "Automatic", "Fixed Mapping", "Custom Mapping" },
        displayDescriptions = { "Infer column types from the first document.", "Create an id column and a data column that stores the document as JSON.", "Define how fields are mapped to columns." },
        shortDescription = "How the schema of the output table gets defined.")
@CastSetting(key = "columns", displayName = "Fields to Columns", defaultType = PolyType.BIGINT, pos = 1, subGroup = "manual",
        allowTarget = true, duplicateSource = true, allowJson = true, subPointer = "mode", subValues = { "\"manual\"" },
        shortDescription = "Specify how (sub)fields are mapped to columns.")
@BoolSetting(key = "includeId", displayName = "Include Document ID", pos = 2,
        subPointer = "mode", subValues = { "\"auto\"" },
        shortDescription = "Whether to insert a column for the document ID.", defaultValue = false)
@EnumSetting(key = "handleMissing", displayName = "Missing Field Handling", options = { "null", "skip", "fail" }, defaultValue = "fail",
        pos = 3, style = EnumStyle.RADIO_BUTTON, subGroup = "manual",
        displayOptions = { "Use Null", "Skip Document", "Fail Execution" }, subPointer = "mode", subValues = { "auto", "\"manual\"" },
        shortDescription = "Determines the strategy for handling documents where a field is missing.")
@BoolSetting(key = "unspecified", displayName = "Column for Unspecified Fields", defaultValue = false, pos = 4, subGroup = "manual",
        subPointer = "mode", subValues = { "\"manual\"" },
        shortDescription = "Adds a column containing any remaining unspecified (top-level) fields as JSON.")
@SuppressWarnings("unused")
public class DocToRelActivity implements Activity, Pipeable {

    public static final String UNSPECIFIED = "_other";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.allPresent() ) {
            String mode = settings.getString( "mode" );
            if ( !mode.equals( "auto" ) ) {
                AlgDataType type = getType(
                        mode,
                        settings.getOrThrow( "columns", CastValue.class ),
                        settings.getBool( "unspecified" )
                );
                return RelType.of( type ).asOutTypes();
            }
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        String mode = settings.getString( "mode" );
        if ( !mode.equals( "auto" ) ) {
            Pipeable.super.execute( inputs, settings, ctx );
            return;
        }

        // auto mode
        DocReader reader = (DocReader) inputs.get( 0 );
        Iterator<PolyDocument> it = reader.getDocIterator();
        if ( !it.hasNext() ) {
            // empty input
            ctx.logWarning( "Input collection is empty, unable to infer output type." );
            AlgDataType type = ActivityUtils.getBuilder()
                    .add( StorageManager.PK_COL, null, PolyType.BIGINT )
                    .build();
            ctx.createRelWriter( 0, type );
            return;
        }
        CastValue cols = getAutoCols( it.next(), settings.getBool( "includeId" ) );
        AlgDataType type = ActivityUtils.addPkCol( cols.asAlgDataType() );

        autoWrite( reader, ctx.createRelWriter( 0, type ), cols, settings.getString( "handleMissing" ), ctx );
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "mode", StringValue.class ).map( m -> !m.getValue().equals( "auto" ) );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType(
                settings.getString( "mode" ),
                settings.get( "columns", CastValue.class ),
                settings.getBool( "unspecified" )
        );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        ctx.logInfo( "Output type: " + output.getType() );
        if ( settings.getString( "mode" ).equals( "fixed" ) ) {
            pipe( inputs.get( 0 ), output, ctx );
        } else {
            // manual mode
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

        for ( List<PolyValue> tuple : input ) {
            PolyDocument doc = tuple.get( 0 ).asDocument();
            List<PolyValue> row = new ArrayList<>();
            row.add( pkVal );

            if ( addFromCast( row, doc, cols, missingStrategy ) ) {
                continue;
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


    private void autoWrite( DocReader reader, RelWriter writer, CastValue cols, String missingStrategy, ExecutionContext ctx ) throws Exception {
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key
        long inCount = reader.getDocCount();
        long countDelta = Math.max( inCount / 100, 1 );
        long count = 0;

        for ( PolyDocument doc : reader.getDocIterable() ) {
            List<PolyValue> row = new ArrayList<>();
            row.add( pkVal );

            if ( addFromCast( row, doc, cols, missingStrategy ) ) {
                continue;
            }

            writer.write( row );
            count++;
            if ( count % countDelta == 0 ) {
                ctx.updateProgress( (double) count / inCount );
            }
            ctx.checkInterrupted();
        }
    }


    /**
     * @return true if the document should be skipped
     */
    protected static boolean addFromCast( List<PolyValue> row, PolyDocument doc, CastValue cols, String missingStrategy ) {
        for ( SingleCast col : cols.getCasts() ) {
            PolyValue value;
            try {
                value = Objects.requireNonNull( ActivityUtils.getSubValue( doc, col.getSource() ) );
            } catch ( Exception e ) {
                switch ( missingStrategy ) {
                    case "null" -> {
                        value = col.getNullValue();
                    }
                    case "skip" -> {
                        return true;
                    }
                    case "fail" -> throw new GenericRuntimeException( "Missing field '" + col.getSource() +
                            "' for '" + doc.toJson() + "'" );
                    default -> throw new IllegalStateException( "ignored" );
                }
            }
            row.add( col.castValue( value ) );
        }
        return false;
    }


    private AlgDataType getType( String mode, CastValue cols, boolean includeUnspecified ) throws InvalidSettingException {
        assert !mode.equals( "auto" ); // in auto, the type cannot be statically determined
        if ( mode.equals( "fixed" ) ) {
            return ActivityUtils.getBuilder()
                    .add( StorageManager.PK_COL, null, factory.createPolyType( PolyType.BIGINT ) )
                    .add( DocumentType.DOCUMENT_ID, null, PolyType.VARCHAR, 24 )
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


    private CastValue getAutoCols( PolyDocument document, boolean includeId ) {
        List<SingleCast> casts = new ArrayList<>();
        if ( includeId ) {
            casts.add( SingleCast.of( DocumentType.DOCUMENT_ID, PolyType.VARCHAR, 24, false ) );
        }

        Set<PolyType> toJsonTypes = Set.of( PolyType.ARRAY, PolyType.MAP, PolyType.DOCUMENT, PolyType.JSON );
        for ( Entry<PolyString, PolyValue> entry : document.entrySet() ) {
            String key = entry.getKey().value;
            if ( key.equals( DocumentType.DOCUMENT_ID ) ) {
                continue;
            }
            PolyValue value = entry.getValue();
            SingleCast cast;
            if ( toJsonTypes.contains( value.type ) ) {
                cast = SingleCast.of( key );
            } else if ( PolyType.CHAR_TYPES.contains( value.type ) ) {
                cast = SingleCast.of( key, PolyType.TEXT, true ); // cannot use VARCHAR since we don't know the length
            } else {
                cast = SingleCast.of( key, value.type, true );
            }
            casts.add( cast );
        }

        CastValue castValue = new CastValue( casts );
        castValue.validate( false, false, true, false );
        return castValue;
    }

}
