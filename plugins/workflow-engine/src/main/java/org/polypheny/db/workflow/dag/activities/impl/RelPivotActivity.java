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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@Slf4j
@ActivityDefinition(type = "relPivot", displayName = "Pivot Table", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL, description = "The pivoted input table.") },
        shortDescription = "Pivots the input table along the specified pivot column. The rows and pivot columns are output in arbitrary order."
)
@FieldSelectSetting(key = "group", displayName = "Group By Column(s)", simplified = true, targetInput = 0, pos = 0,
        shortDescription = "The fixed columns to group the rows by.")
@StringSetting(key = "pivot", displayName = "Pivot Column", pos = 1,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, nonBlank = true,
        shortDescription = "The column whose unique values become the column names in the output table.")
@StringSetting(key = "value", displayName = "Value Column", pos = 2,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, nonBlank = true,
        shortDescription = "The column containing the values to be aggregated and placed in the pivoted columns.")
@EnumSetting(key = "aggregation", displayName = "Aggregation Function", pos = 3,
        options = { "FIRST", "SUM", "AVG", "COUNT" }, defaultValue = "FIRST",
        displayOptions = { "First Value", "Sum", "Average", "Count" },
        shortDescription = "The function to be used for summarizing multiple values per pivot column.")

@SuppressWarnings("unused")
public class RelPivotActivity implements Activity {

    private final List<String> columns = new ArrayList<>();
    private final Map<List<PolyValue>, Map<String, PolyValue>> reducedValues = new LinkedHashMap<>(); // maps group-by cols to a pivot->value map
    private final Map<List<PolyValue>, Map<String, Integer>> valueCounts = new LinkedHashMap<>(); // maps group-by cols to a pivot->count map

    private final double progress1 = 0.1; // distinct pivot values
    private final double progress2 = 0.75; // computed result


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        AlgDataType type = inTypes.get( 0 ).getNullableType();
        if ( settings.allPresent() && type != null ) {
            List<String> group = settings.getOrThrow( "group", FieldSelectValue.class ).getInclude();
            if ( group.isEmpty() ) {
                throw new InvalidSettingException( "At least one group column must be specified", "group" );
            }
            String pivot = settings.getString( "pivot" );
            String value = settings.getString( "value" );
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
            if ( !cols.contains( pivot ) ) {
                throw new InvalidSettingException( "Unknown pivot column: " + pivot, "pivot" );
            } else if ( visitedCols.contains( pivot ) ) {
                throw new InvalidSettingException( "Duplicate column: " + pivot, "pivot" );
            } else if ( type.getField( pivot, true, false ).getType().getPolyType().getFamily() != PolyTypeFamily.CHARACTER ) {
                throw new InvalidSettingException( "The pivot column must have a textual type", "pivot" );
            }
            visitedCols.add( pivot );
            if ( !cols.contains( value ) ) {
                throw new InvalidSettingException( "Unknown value column: " + value, "value" );
            } else if ( visitedCols.contains( value ) ) {
                throw new InvalidSettingException( "Duplicate column: " + value, "value" );
            }
        }
        return UnknownType.ofRel().asOutTypes(); // the columns depend on the data
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        List<String> group = settings.get( "group", FieldSelectValue.class ).getInclude();
        String pivot = settings.getString( "pivot" );
        String value = settings.getString( "value" );
        AggFunction agg = AggFunction.valueOf( settings.getString( "aggregation" ) );
        RelReader reader = (RelReader) inputs.get( 0 );

        setColumns( reader, group, pivot );
        ctx.updateProgress( progress1 );
        ctx.logInfo( "Pivot column values: " + columns );
        AlgDataType type = getType( reader.getTupleType(), group, value, agg );

        computePivot( reader, group, pivot, value, agg );
        ctx.updateProgress( progress2 );
        ctx.logInfo( "Computed aggregated values. Size: " + valueCounts.size() );
        writeResult( ctx.createRelWriter( 0, type ), group, agg, ctx );
    }


    private void setColumns( RelReader reader, List<String> group, String pivot ) {
        CheckpointQuery distinctQuery = CheckpointQuery.builder()
                .query( "SELECT DISTINCT " + QueryUtils.quote( pivot ) + " FROM " + CheckpointQuery.ENTITY() )
                .queryLanguage( "SQL" )
                .build();

        for ( List<PolyValue> row : reader.getIterableFromQuery( distinctQuery ).right ) {
            PolyString pivotValue = row.get( 0 ).asString();
            columns.add( pivotValue.value );
        }
    }


    private void computePivot( RelReader reader, List<String> groupCol, String pivotCol, String valueCol, AggFunction agg ) {
        List<String> allCols = new ArrayList<>( groupCol );
        allCols.add( pivotCol );
        allCols.add( valueCol );
        CheckpointQuery query = CheckpointQuery.builder()
                .query( "SELECT " + QueryUtils.quoteAndJoin( allCols ) + " FROM " + CheckpointQuery.ENTITY() )
                .queryLanguage( "SQL" )
                .build();

        int groupSize = groupCol.size();
        for ( List<PolyValue> row : reader.getIterableFromQuery( query ).right ) {
            List<PolyValue> group = row.subList( 0, groupSize );
            String pivot = row.get( groupSize ).asString().value;
            PolyValue value = row.get( groupSize + 1 );

            valueCounts.computeIfAbsent( group, k -> new HashMap<>() ).merge( pivot, 1, Integer::sum );
            Map<String, PolyValue> valueMap = reducedValues.computeIfAbsent( group, k -> new HashMap<>() );
            try {
                switch ( agg ) {
                    case FIRST -> valueMap.putIfAbsent( pivot, value );
                    case SUM, AVG -> valueMap.merge( pivot, value, ( v1, v2 ) -> v1.asNumber().plus( v2.asNumber() ) );
                }
            } catch ( NullPointerException e ) {
                throw new GenericRuntimeException( "NullPointerException in row: " + row, e );
            }
        }
    }


    private void writeResult( RelWriter relWriter, List<String> groupCols, AggFunction agg, ExecutionContext ctx ) throws ExecutorException {
        int rowSize = groupCols.size() + columns.size();
        long totalCount = reducedValues.size();
        long countDelta = Math.max( totalCount / 100, 1 );
        long count = 0;
        for ( Entry<List<PolyValue>, Map<String, PolyValue>> entry : reducedValues.entrySet() ) {
            List<PolyValue> row = new ArrayList<>( rowSize );
            List<PolyValue> group = entry.getKey();
            Map<String, PolyValue> reducedMap = entry.getValue();
            row.addAll( group );

            Map<String, Integer> countMap = valueCounts.get( group );
            for ( String pivot : columns ) {
                PolyValue value = null;
                try {
                    value = switch ( agg ) {
                        case FIRST, SUM -> reducedMap.get( pivot );
                        case AVG -> reducedMap.get( pivot ).asNumber().divide( PolyLong.of( countMap.get( pivot ) ) );
                        case COUNT -> PolyLong.of( countMap.get( pivot ) );
                    };
                } catch ( Exception ignored ) {
                }
                if ( value == null ) {
                    if ( agg == AggFunction.FIRST || agg == AggFunction.AVG ) {
                        value = PolyNull.NULL;
                    } else {
                        value = PolyLong.of( 0 ); // for COUNT and SUM, 0 makes sense
                    }
                }
                row.add( value );
            }
            relWriter.wWithoutPk( row );
            count++;
            if ( count % countDelta == 0 ) {
                ctx.updateProgress( (double) count / totalCount * (1 - progress2) + progress2 ); // writing only is part of progress
            }
            ctx.checkInterrupted();
        }
    }


    private AlgDataType getType( AlgDataType inType, List<String> group, String value, AggFunction agg ) {
        Builder builder = ActivityUtils.getBuilder()
                .add( StorageManager.PK_COL, null, PolyType.BIGINT );

        for ( String g : group ) {
            builder.add( inType.getField( g, true, false ) );
        }
        AlgDataType valueType = agg == AggFunction.COUNT ?
                factory.createPolyType( PolyType.BIGINT ) :
                inType.getField( value, true, false ).getType(); // use input type as output type
        for ( String pivot : columns ) {
            builder.add( pivot, null, valueType ).nullable( true );
        }
        AlgDataType outType = builder.uniquify().build();
        ActivityUtils.validateFieldNames( outType.getFieldNames() );
        return outType;
    }


    @Override
    public void reset() {
        columns.clear();
        reducedValues.clear();
        valueCounts.clear();
    }


    private enum AggFunction {
        FIRST,
        SUM,
        AVG,
        COUNT
    }

}
