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
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
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
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relUnpivot", displayName = "Unpivot Table", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "A table in pivoted (wide) form.") },
        outPorts = { @OutPort(type = PortType.REL, description = "The unpivoted (melted) input table.") },
        shortDescription = "Unpivots the input table with any non-identifier column being interpreted as a variable column."
)
@FieldSelectSetting(key = "group", displayName = "Identifier Column(s)", simplified = true, targetInput = 0, pos = 0,
        shortDescription = "The fixed columns that contain the identifier values ('group by' columns).")
@StringSetting(key = "pivot", displayName = "Variable Column Name", pos = 1,
        defaultValue = "variable", nonBlank = true,
        shortDescription = "The name of the column that will contain the pivot column names of the input table.")
@StringSetting(key = "value", displayName = "Value Column Name", pos = 2,
        defaultValue = "value", nonBlank = true,
        shortDescription = "The name of the column containing the unpivoted values.")

@SuppressWarnings("unused")
public class RelUnpivotActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        if ( settings.allPresent() ) {
            List<String> group = settings.getOrThrow( "group", FieldSelectValue.class ).getInclude();
            if ( group.isEmpty() ) {
                throw new InvalidSettingException( "At least one identifier column must be specified", "group" );
            }

            if ( type != null ) {
                if ( type.getFieldCount() == group.size() ) {
                    throw new InvalidSettingException( "At least one column most not be an identifier column", "group" );
                }

                Set<String> cols = new HashSet<>( type.getFieldNames() );
                Set<String> visitedCols = new HashSet<>();
                for ( String col : group ) {
                    if ( !cols.contains( col ) ) {
                        throw new InvalidSettingException( "Unknown group column: " + col, "group" );
                    } else if ( visitedCols.contains( col ) ) {
                        throw new InvalidSettingException( "Duplicate column: " + col, "group" );
                    }
                    visitedCols.add( col );
                }

                return RelType.of( getType( type, group, settings.getString( "pivot" ), settings.getString( "value" ) ) ).asOutTypes();
            }

        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType( inTypes.get( 0 ),
                settings.get( "group", FieldSelectValue.class ).getInclude(),
                settings.getString( "pivot" ),
                settings.getString( "value" ) );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        List<String> group = settings.get( "group", FieldSelectValue.class ).getInclude();
        AlgDataType inType = inputs.get( 0 ).getType();
        List<Integer> groupIndexes = group.stream().map( g -> inType.getFieldNames().indexOf( g ) ).toList();
        List<Integer> indexes = IntStream.range( 1, inType.getFieldCount() ) // start at 1 to skip PK_KEY
                .filter( i -> !group.contains( inType.getFieldNames().get( i ) ) )
                .boxed().toList();

        Map<Integer, String> pivotNames = indexes.stream().collect( Collectors.toMap( i -> i, i -> inType.getFieldNames().get( i ) ) );
        PolyValue pkVal = PolyLong.of( 0 ); // placeholder for primary key
        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            List<PolyValue> groupValues = groupIndexes.stream().map( row::get ).toList();
            for ( int i : indexes ) {
                String name = pivotNames.get( i );
                List<PolyValue> outRow = new ArrayList<>();
                outRow.add( pkVal );
                outRow.addAll( groupValues );
                outRow.add( PolyString.of( name ) );
                outRow.add( row.get( i ) );
                if ( !output.put( outRow ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    private AlgDataType getType( AlgDataType inType, List<String> group, String pivotCol, String valueCol ) {
        Builder builder = ActivityUtils.getBuilder()
                .add( StorageManager.PK_COL, null, PolyType.BIGINT );

        for ( String g : group ) {
            builder.add( inType.getField( g, true, false ) );
        }

        int maxLen = 0;
        List<AlgDataType> valueTypes = new ArrayList<>();
        for ( AlgDataTypeField field : inType.getFields() ) {
            if ( group.contains( field.getName() ) ) {
                continue;
            }
            int nameLen = field.getName().length();
            if ( nameLen > maxLen ) {
                maxLen = nameLen;
            }
            valueTypes.add( field.getType() );
        }
        builder.add( pivotCol, null, PolyType.VARCHAR, maxLen );
        builder.add( valueCol, null, factory.leastRestrictive( valueTypes ) ).nullable( true );

        AlgDataType outType = builder.uniquify().build();
        ActivityUtils.validateFieldNames( outType.getFieldNames() );
        return outType;
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        long inRows = Activity.computeTupleCountSum( inCounts );
        if ( inRows >= 0 ) {
            int pivotCols = inTypes.get( 0 ).getFieldCount() - settings.get( "group", FieldSelectValue.class ).getInclude().size();
            return inRows * pivotCols;
        }
        return -1;
    }

}
