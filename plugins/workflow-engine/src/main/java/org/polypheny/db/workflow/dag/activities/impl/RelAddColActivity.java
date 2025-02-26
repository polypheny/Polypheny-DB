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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
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
import org.polypheny.db.workflow.dag.annotations.CastSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.CastValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relAddCol", displayName = "Add Constant Column", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "Adds or replaces a column filled with a constant value."
)

@BoolSetting(key = "replace", displayName = "Replace Column", pos = 0,
        shortDescription = "Replace an existing column instead of appending a new one.")

@CastSetting(key = "newCol", displayName = "Column to Add", defaultType = PolyType.BIGINT, targetInput = -1, singleCast = true,
        subPointer = "replace", subValues = { "false" }, pos = 1,
        shortDescription = "Specify the type of the column.")
@StringSetting(key = "colName", displayName = "Column to Replace", autoCompleteType = AutoCompleteType.FIELD_NAMES, pos = 2,
        subPointer = "replace", subValues = { "true" },
        shortDescription = "The name of the column to replace.")

@StringSetting(key = "value", displayName = "Value", pos = 3,
        shortDescription = "The constant value to insert in all rows.")

@SuppressWarnings("unused")
public class RelAddColActivity implements Activity, Pipeable, Fusable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview inType = inTypes.get( 0 );
        if ( inType instanceof RelType relType && settings.keysPresent( "replace", "newCol", "colName" ) ) {
            if ( settings.getBool( "replace" ) ) {
                String colName = settings.getString( "colName" );
                if ( !inType.getNullableType().getFieldNames().contains( colName ) ) {
                    throw new InvalidSettingException( "Column '" + colName + "' does not exist in the input table", "colName" );
                }
                if ( colName.equals( PK_COL ) ) {
                    throw new InvalidSettingException( "Cannot replace the primary key column.", "colName" );
                }
                return inType.asOutTypes();
            } else {
                CastValue col = settings.getOrThrow( "newCol", CastValue.class );
                AlgDataType type = getType( inType.getNullableType(), col );
                return RelType.of( type ).asOutTypes();
            }
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        if ( settings.getBool( "replace" ) ) {
            return inTypes.get( 0 );
        }
        return getType( inTypes.get( 0 ), settings.get( "newCol", CastValue.class ) );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        InputPipe input = inputs.get( 0 );
        String value = settings.getString( "value" );

        if ( settings.getBool( "replace" ) ) {
            int i = input.getType().getFieldNames().indexOf( settings.getString( "colName" ) );
            AlgDataType fieldType = input.getType().getFields().get( i ).getType();
            PolyValue polyValue = ActivityUtils.stringToPolyValue( value, fieldType.getPolyType() );
            for ( List<PolyValue> tuple : input ) {
                List<PolyValue> row = new ArrayList<>( tuple );
                row.set( i, polyValue );
                if ( !output.put( row ) ) {
                    finish( inputs );
                    return;
                }
            }
        } else {
            CastValue col = settings.get( "newCol", CastValue.class );
            PolyValue polyValue = col.getCasts().get( 0 ).castValue( PolyString.of( value ) );
            for ( List<PolyValue> tuple : input ) {
                List<PolyValue> row = new ArrayList<>( tuple );
                row.add( polyValue );
                if ( !output.put( row ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgDataType inType = inputs.get( 0 ).getTupleType();
        AlgDataType outType = inType;
        String value = settings.getString( "value" );

        List<RexNode> projects = IntStream.range( 0, inType.getFieldCount() )
                .mapToObj( i -> new RexIndexRef( i, inType.getFields().get( i ).getType() ) )
                .collect( Collectors.toCollection( ArrayList::new ) );

        if ( settings.getBool( "replace" ) ) {
            int i = inType.getFieldNames().indexOf( settings.getString( "colName" ) );
            AlgDataType fieldType = inType.getFields().get( i ).getType();
            PolyValue polyValue = ActivityUtils.stringToPolyValue( value, fieldType.getPolyType() );
            projects.set( i, ActivityUtils.getRexLiteral( polyValue, fieldType ) );
        } else {
            CastValue col = settings.get( "newCol", CastValue.class );
            AlgDataType fieldType = col.getCasts().get( 0 ).getAlgDataType();
            PolyValue polyValue = col.getCasts().get( 0 ).castValue( PolyString.of( value ) );
            projects.add( ActivityUtils.getRexLiteral( polyValue, fieldType ) );
            outType = getType( inType, col );
        }
        return LogicalRelProject.create( inputs.get( 0 ), projects, outType );
    }


    private AlgDataType getType( AlgDataType type, CastValue col ) throws InvalidSettingException {
        AlgDataType outType = ActivityUtils.concatTypes( type, col.asAlgDataType() );
        Optional<String> invalid = ActivityUtils.findInvalidFieldName( outType.getFieldNames() );
        if ( invalid.isPresent() ) {
            throw new InvalidSettingException( "Invalid column name: " + invalid.get(), "rename" );
        }
        return outType;
    }

}
