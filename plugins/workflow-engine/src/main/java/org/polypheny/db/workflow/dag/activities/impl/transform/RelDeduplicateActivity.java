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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;
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
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
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
@ActivityDefinition(type = "relDeduplicate", displayName = "Remove Duplicate Rows", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.REL, description = "The input table that possibly contains duplicate rows") },
        outPorts = { @OutPort(type = PortType.REL, description = "The output table with duplicates removed") },
        shortDescription = "Removes rows with duplicate values in specified columns. Only the first row is kept for each unique combination of values in the detection columns."
)
@FieldSelectSetting(key = "columns", displayName = "Duplicate Detection Columns", simplified = true, targetInput = 0, pos = 0,
        shortDescription = "The columns used to determine whether a row is a duplicate. If left empty, all columns are used.")

@SuppressWarnings("unused")
public class RelDeduplicateActivity implements Activity, Pipeable, Fusable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        if ( settings.keysPresent( "columns" ) && type != null ) {
            Set<String> colNames = new HashSet<>( type.getFieldNames() );
            Set<String> visitedCols = new HashSet<>();
            for ( String col : settings.getOrThrow( "columns", FieldSelectValue.class ).getInclude() ) {
                if ( !colNames.contains( col ) ) {
                    throw new InvalidSettingException( "Unknown column: " + col, "columns" );
                } else if ( visitedCols.contains( col ) ) {
                    throw new InvalidSettingException( "Duplicate column: " + col, "columns" );
                } else if ( col.equals( PK_COL ) ) {
                    throw new InvalidSettingException( "Column must not be equal to the primary key column: " + col, "columns" );
                }
                visitedCols.add( col );
            }
        }
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        List<String> columns = settings.get( "columns", FieldSelectValue.class ).getInclude();
        if ( columns.isEmpty() || columns.size() == inputs.get( 0 ).getTupleType().getFieldCount() - 1 ) {
            Fusable.super.execute( inputs, settings, ctx );
            return;
        }
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "columns" ) ) {
            List<String> columns = settings.getOrThrow( "columns", FieldSelectValue.class ).getInclude();
            if ( columns.isEmpty() ) {
                return Optional.of( true );
            }
            if ( inTypes.get( 0 ) instanceof RelType relType ) {
                return Optional.of( relType.getNullableType().getFieldCount() - 1 == columns.size() );
            }
        }
        return Optional.empty();
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        ImmutableBitSet groupSet = ImmutableBitSet.range( 1, inputs.get( 0 ).getTupleType().getFieldCount() ); // skip PK_COL
        AlgNode aggregate = LogicalRelAggregate.create( inputs.get( 0 ), groupSet, null, List.of() );
        return ActivityUtils.addPkCol( aggregate, cluster );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        AlgDataType inType = inputs.get( 0 ).getType();
        List<String> columns = settings.get( "columns", FieldSelectValue.class ).getInclude();
        if ( columns.isEmpty() ) {
            columns = inType.getFieldNames().stream().filter( c -> !c.equals( PK_COL ) ).toList();
        }
        List<Integer> groupIndexes = columns.stream().map( c -> inType.getFieldNames().indexOf( c ) ).toList();
        Set<List<PolyValue>> uniqueValues = new HashSet<>();

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            List<PolyValue> groupValues = groupIndexes.stream().map( row::get ).toList();
            if ( uniqueValues.contains( groupValues ) ) {
                continue;
            }
            uniqueValues.add( groupValues );
            if ( !output.put( row ) ) {
                finish( inputs );
                return;
            }
        }
    }

}
