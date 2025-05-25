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

import static org.polypheny.db.workflow.dag.activities.impl.crossmodel.LpgNodesToDocActivity.ID_FIELD;
import static org.polypheny.db.workflow.dag.activities.impl.crossmodel.LpgNodesToDocActivity.LABEL_FIELD;
import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
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
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.CastSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.settings.CastValue;
import org.polypheny.db.workflow.dag.settings.CastValue.SingleCast;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "lpgNodesToRel", displayName = "Nodes to Table", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.RELATIONAL, ActivityCategory.CROSS_MODEL },
        inPorts = { @InPort(type = PortType.LPG, description = "The input graph.") },
        outPorts = { @OutPort(type = PortType.REL, description = "The output table where each row corresponds to a node in the graph.") },
        shortDescription = "Maps nodes of a graph to rows in a table."
)

@DefaultGroup(subgroups = { @Subgroup(key = "manual", displayName = "Custom Mapping") })

@FieldSelectSetting(key = "labels", displayName = "Target Nodes", simplified = true, pos = 0, group = ADVANCED_GROUP,
        targetInput = 0, forLabels = true,
        shortDescription = "Specify the nodes to map by their label(s). If no label is specified, all nodes are mapped to rows.")
@EnumSetting(key = "mode", displayName = "Property Mapping", style = EnumStyle.RADIO_BUTTON, pos = 1,
        options = { "auto", "fixed", "manual" }, defaultValue = "auto",
        displayOptions = { "Automatic", "Fixed Mapping", "Custom Mapping" },
        displayDescriptions = { "Infer column types from the first node.", "Add a column that stores all properties as JSON.", "Define how properties are mapped to columns." },
        shortDescription = "How the schema of the output table gets defined.")

@BoolSetting(key = "includeLabels", displayName = "Include Labels", defaultValue = true, pos = 2, group = ADVANCED_GROUP,
        shortDescription = "Whether to insert a column '" + LABEL_FIELD + "' that contains the node labels.")
@BoolSetting(key = "includeId", displayName = "Include Node ID", pos = 3, group = ADVANCED_GROUP,
        shortDescription = "Whether to insert a '" + ID_FIELD + "' column.")

@CastSetting(key = "columns", displayName = "Properties to Columns", defaultType = PolyType.BIGINT, pos = 1, subGroup = "manual",
        allowTarget = true, duplicateSource = true, allowJson = true, targetInput = 0,
        subPointer = "mode", subValues = { "\"manual\"" },
        shortDescription = "Specify how properties are mapped to columns.")
@EnumSetting(key = "handleMissing", displayName = "Missing Property Handling", options = { "null", "skip", "fail" }, defaultValue = "fail",
        pos = 2, style = EnumStyle.RADIO_BUTTON, subGroup = "manual",
        displayOptions = { "Use Null", "Skip Node", "Fail Execution" }, subPointer = "mode", subValues = { "auto", "\"manual\"" },
        shortDescription = "Determines the strategy for handling nodes where a specified property is missing.")
@BoolSetting(key = "unspecified", displayName = "Column for Unspecified Properties", defaultValue = false, pos = 3, subGroup = "manual",
        subPointer = "mode", subValues = { "\"manual\"" },
        shortDescription = "Adds a column containing any remaining unspecified properties as JSON.")
@SuppressWarnings("unused")
public class LpgNodesToRelActivity implements Activity, Pipeable {

    static final String PROPERTIES_FIELD = "properties";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.allPresent() ) {
            String mode = settings.getString( "mode" );
            if ( !mode.equals( "auto" ) ) {
                AlgDataType type = getType(
                        mode,
                        settings.getOrThrow( "columns", CastValue.class ),
                        settings.getBool( "unspecified" ),
                        settings.getBool( "includeLabels" ),
                        settings.getBool( "includeId" )
                );
                return RelType.of( type ).asOutTypes();
            }
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        String mode = settings.getString( "mode" );
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean includeLabels = settings.getBool( "includeLabels" );
        boolean includeId = settings.getBool( "includeId" );
        if ( !mode.equals( "auto" ) ) {
            Pipeable.super.execute( inputs, settings, ctx );
            return;
        }

        // auto mode
        LpgReader reader = (LpgReader) inputs.get( 0 );
        Iterator<PolyNode> it = reader.getNodeIterator();
        if ( !it.hasNext() ) {
            // empty input
            ctx.logWarning( "Input graph is empty, unable to infer output type." );
            AlgDataType type = ActivityUtils.getBuilder()
                    .add( StorageManager.PK_COL, null, PolyType.BIGINT )
                    .build();
            ctx.createRelWriter( 0, type );
            return;
        }
        CastValue cols = getAutoCols( it.next().properties );
        AlgDataType type = getType( mode, cols, false, includeLabels, includeId );

        autoWrite( reader, ctx.createRelWriter( 0, type ), cols, settings.getString( "handleMissing" ), includeLabels, includeId, labels, ctx );
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
                settings.getBool( "unspecified" ),
                settings.getBool( "includeLabels" ),
                settings.getBool( "includeId" )
        );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        Set<String> labels = new HashSet<>( settings.get( "labels", FieldSelectValue.class ).getInclude() );
        boolean includeLabels = settings.getBool( "includeLabels" );
        boolean includeId = settings.getBool( "includeId" );
        ctx.logInfo( "Output type: " + output.getType() );
        if ( settings.getString( "mode" ).equals( "fixed" ) ) {
            pipe( inputs.get( 0 ).asLpgInputPipe(), output, includeLabels, includeId, labels, ctx );
        } else {
            // manual mode
            pipe( inputs.get( 0 ).asLpgInputPipe(),
                    output,
                    settings.get( "columns", CastValue.class ),
                    settings.getBool( "unspecified" ),
                    settings.getString( "handleMissing" ),
                    includeLabels,
                    includeId,
                    labels,
                    ctx
            );
        }
    }


    private void autoWrite( LpgReader reader, RelWriter writer, CastValue cols, String missingStrategy, boolean includeLabels, boolean includeId, Set<String> labels, ExecutionContext ctx ) throws Exception {
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key
        long inCount = reader.getNodeCount();
        long countDelta = Math.max( inCount / 100, 1 );
        long count = 0;

        for ( PolyNode node : reader.getNodeIterable() ) {
            if ( !ActivityUtils.matchesLabelList( node, labels ) ) {
                continue;
            }
            List<PolyValue> row = new ArrayList<>();
            row.add( pkVal );
            addOptionalCols( row, node, includeLabels, includeId );
            if ( DocToRelActivity.addFromCast( row, PolyDocument.ofDocument( node.properties ), cols, missingStrategy ) ) {
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


    private void pipe( LpgInputPipe input, OutputPipe output, boolean includeLabels, boolean includeId, Set<String> labels, PipeExecutionContext ctx ) throws Exception {
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key
        for ( PolyNode node : input.getNodeIterable() ) {
            if ( !ActivityUtils.matchesLabelList( node, labels ) ) {
                continue;
            }
            List<PolyValue> row = new ArrayList<>();
            row.add( pkVal );
            addOptionalCols( row, node, includeLabels, includeId );
            row.add( node.properties.toPolyJson() );

            if ( !output.put( row ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private void pipe(
            LpgInputPipe input, OutputPipe output, CastValue cols, boolean includeUnspecified, String missingStrategy,
            boolean includeLabels, boolean includeId, Set<String> labels, PipeExecutionContext ctx ) throws Exception {
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key
        Set<String> props = cols.getCasts().stream().map( c -> c.getSource().split( "\\." )[0] ).collect( Collectors.toSet() );

        for ( PolyNode node : input.getNodeIterable() ) {
            if ( !ActivityUtils.matchesLabelList( node, labels ) ) {
                continue;
            }
            List<PolyValue> row = new ArrayList<>();
            row.add( pkVal );
            addOptionalCols( row, node, includeLabels, includeId );
            if ( DocToRelActivity.addFromCast( row, PolyDocument.ofDocument( node.properties ), cols, missingStrategy ) ) {
                continue;
            }

            if ( includeUnspecified ) {
                addUnspecified( row, node.properties, props );
            }

            if ( !output.put( row ) ) {
                input.finishIteration();
                return;
            }
        }
    }


    private AlgDataType getType( String mode, CastValue cols, boolean includeUnspecified, boolean includeLabels, boolean includeId ) throws InvalidSettingException {
        Builder builder = ActivityUtils.getBuilder()
                .add( StorageManager.PK_COL, null, factory.createPolyType( PolyType.BIGINT ) );
        if ( includeId ) {
            builder.add( ID_FIELD, null, PolyType.VARCHAR, 36 );
        }
        if ( includeLabels ) {
            builder.add( LABEL_FIELD, null, PolyType.TEXT );
        }

        if ( mode.equals( "fixed" ) ) {
            return builder
                    .add( PROPERTIES_FIELD, null, PolyType.TEXT )
                    .build();
        }

        AlgDataType type = ActivityUtils.concatTypes( builder.build(), cols.asAlgDataType() );
        if ( mode.equals( "manual" ) ) {
            if ( includeUnspecified ) {
                type = ActivityUtils.appendField( type, DocToRelActivity.UNSPECIFIED, factory.createPolyType( PolyType.TEXT ) );
            }
            Optional<String> invalid = ActivityUtils.findInvalidFieldName( type.getFieldNames() );
            if ( invalid.isPresent() ) {
                throw new InvalidSettingException( "Invalid column name: " + invalid.get(), "columns" );
            }
        }
        return type;
    }


    protected static void addOptionalCols( List<PolyValue> row, GraphPropertyHolder holder, boolean includeLabels, boolean includeId ) {
        if ( includeId ) {
            row.add( holder.getId() );
        }
        if ( includeLabels ) {
            if ( holder.getLabels().size() == 1 ) {
                row.add( holder.getLabels().get( 0 ) );
            } else {
                row.add( holder.getLabels().toPolyJson() );
            }
        }
    }


    protected static CastValue getAutoCols( PolyDictionary props ) {
        List<SingleCast> casts = new ArrayList<>();

        Set<PolyType> toJsonTypes = Set.of( PolyType.ARRAY, PolyType.MAP, PolyType.DOCUMENT, PolyType.JSON );
        for ( Entry<PolyString, PolyValue> entry : props.entrySet() ) {
            String key = entry.getKey().value;
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


    protected static void addUnspecified( List<PolyValue> row, PolyDictionary props, Set<String> specifiedProps ) {
        Map<PolyString, PolyValue> unspecified = new HashMap<>();
        for ( Entry<PolyString, PolyValue> entry : props.entrySet() ) {
            String key = entry.getKey().value;
            if ( !specifiedProps.contains( key ) ) {
                unspecified.put( entry.getKey(), entry.getValue() );
            }
        }
        if ( unspecified.isEmpty() ) {
            row.add( PolyString.of( null ) );
        } else {
            row.add( PolyDocument.ofDocument( unspecified ).toPolyJson() );
        }
    }


}
