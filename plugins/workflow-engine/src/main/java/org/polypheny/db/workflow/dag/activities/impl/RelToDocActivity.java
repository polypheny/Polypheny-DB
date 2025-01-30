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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "relToDoc", displayName = "Table to Collection", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.REL, description = "The input table") },
        outPorts = { @OutPort(type = PortType.DOC, description = "A collection containing the input table rows as documents.") },
        shortDescription = "Transform the rows of a table into documents."
)

@BoolSetting(key = "skipPk", displayName = "Skip Key Column", defaultValue = true, pos = 0,
        shortDescription = "Do not include the '" + StorageManager.PK_COL + "' column.")
@FieldSelectSetting(key = "jsonCols", displayName = "JSON Columns", simplified = true, reorder = false, targetInput = 0, pos = 1,
        shortDescription = "Specify the text columns to interpret as (escaped) JSON.")
@BoolSetting(key = "fail", displayName = "Fail on Invalid JSON", defaultValue = true, pos = 2,
        shortDescription = "If the execution should fail when a specified JSON-column does not contain valid JSON.")
@BoolSetting(key = "unwrap", displayName = "Unwrap JSON objects", defaultValue = false, pos = 3,
        shortDescription = "If the entry in a JSON column is an object, it gets unwrapped.")

@SuppressWarnings("unused")
public class RelToDocActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof RelType rel ) {
            Optional<FieldSelectValue> select = settings.get( "jsonCols", FieldSelectValue.class );
            if ( select.isPresent() && settings.get( "fail", BoolValue.class ).map( BoolValue::getValue ).orElse( false ) ) {
                List<String> cols = select.get().getInclude();
                List<AlgDataTypeField> fields = rel.getNullableType().getFields();
                for ( AlgDataTypeField field : fields ) {
                    if ( cols.contains( field.getName() ) && !PolyType.STRING_TYPES.contains( field.getType().getPolyType() ) ) {
                        throw new InvalidSettingException( "The selected JSON column '" + field.getName() + "' does not have a string type.", "jsonCols" );
                    }
                }
            }

            return DocType.of( rel.getNullableType().getFieldNames() ).asOutTypes();
        }
        return DocType.of().asOutTypes();

    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        if ( settings.get( "jsonCols", FieldSelectValue.class ).getInclude().isEmpty() ) {
            // TODO: call more efficient fuse implementation
            // Fusable.super.execute( inputs, settings, ctx );
            // return;
        }
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        // TODO: enable when fusion works
        return settings.get( "jsonCols", FieldSelectValue.class ).map( select -> select.getInclude().isEmpty() );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgNode node = inputs.get( 0 );
        if ( settings.getBool( "skipPk" ) ) {
            node = ActivityUtils.removePkCol( node, cluster );
        }
        // TODO: fix rel -> doc
        //return new LogicalDocumentProject( cluster, cluster.traitSetOf( ModelTrait.DOCUMENT ), node, Map.of(), List.of() );
        return LogicalTransformer.create( cluster, List.of( node ), null, ModelTrait.RELATIONAL, ModelTrait.DOCUMENT, getDocType(), true );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        boolean skipPk = settings.getBool( "skipPk" );
        boolean failOnInvalid = settings.getBool( "fail" );
        boolean unwrap = settings.getBool( "unwrap" );
        List<String> included = settings.get( "jsonCols", FieldSelectValue.class ).getInclude();
        List<String> fieldNames = inputs.get( 0 ).getType().getFieldNames();
        Set<Integer> jsonCols = new HashSet<>();
        for ( int i = 0; i < fieldNames.size(); i++ ) {
            String name = fieldNames.get( i );
            if ( included.contains( name ) ) {
                jsonCols.add( i );
            }
        }

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            Map<PolyString, PolyValue> map = new HashMap<>();
            for ( int i = skipPk ? 1 : 0; i < row.size(); i++ ) {
                PolyValue value = row.get( i );
                if ( jsonCols.contains( i ) ) {
                    try {
                        String serialized = value.toJson().replace( "\\\\", "\\" ).replace( "\\\"", "\"" );
                        PolyValue deserialized = PolyValue.fromJson( serialized );
                        if ( unwrap && deserialized instanceof PolyDocument doc ) {
                            map.putAll( doc.map );
                        } else {
                            map.put( PolyString.of( fieldNames.get( i ) ), deserialized );
                        }
                    } catch ( Exception e ) {
                        if ( failOnInvalid ) {
                            throw e;
                        }
                        map.put( PolyString.of( fieldNames.get( i ) ), value );
                    }
                } else {
                    map.put( PolyString.of( fieldNames.get( i ) ), value );
                }
            }
            if ( !output.put( PolyDocument.ofDocument( map ) ) ) {
                inputs.forEach( InputPipe::finishIteration );
                break;
            }
        }
    }

}
