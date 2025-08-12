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

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.CollationSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.CollationValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "relSort", displayName = "Sort / Limit Rows", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.REL, description = "The input table") },
        outPorts = { @OutPort(type = PortType.REL, description = "The table sorted by the specified columns and possibly limited number of rows.") },
        shortDescription = "This activity can be used to both sort a table and limit its number of rows. Note that some adapters do not keep track of the row order.",
        longDescription = """
                This activity can be used to both sort a table and limit its number of rows.
                
                > # Note
                >
                > Some adapters do not keep track of the sort order.
                > This implies that the order only persists until the output is materialized in a checkpoint.
                > If a downstream activity expects sorted input data, consider enabling activity fusion or pipelining. This removes checkpoints between compatible activities.
                """
)
@CollationSetting(key = "sort", displayName = "Sort Columns", shortDescription = "Specify the column(s) to sort the rows by. If no column is selected, the original order is used.", allowRegex = true)
@IntSetting(key = "limit", displayName = "Row Limit", defaultValue = -1, min = -1, shortDescription = "The total number of rows to include, or -1 to include all rows.")
@IntSetting(key = "skip", displayName = "Skip Rows", defaultValue = 0, min = 0, shortDescription = "The number of rows to skip, or 0 to start at the top row.")
@SuppressWarnings("unused")
public class RelSortActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgDataType type = inputs.get( 0 ).getTupleType();
        AlgCollation collation = settings.get( "sort", CollationValue.class ).toAlgCollation( type );
        int limit = settings.getInt( "limit" );
        int skip = settings.getInt( "skip" );

        if ( collation.getFieldCollations().isEmpty() && limit == -1 && skip == 0 ) {
            ctx.logInfo( "Skipping trivial sort" );
            return inputs.get( 0 ); // trivial sort
        }
        RexLiteral limitLiteral = limit == -1 ? null : ActivityUtils.getRexLiteral( limit );
        RexLiteral skipLiteral = skip == 0 ? null : ActivityUtils.getRexLiteral( skip );

        if ( !collation.getFieldCollations().isEmpty() ) {
            ctx.logInfo( "Sorting by columns " + collation.getFieldCollations() );
        }

        return LogicalRelSort.create( inputs.get( 0 ), collation, skipLiteral, limitLiteral );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return inTypes.get( 0 );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int limit = settings.getInt( "limit" );
        int skip = settings.getInt( "skip" );

        long count = 0;

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            count++;
            if ( count <= skip ) {
                continue;
            } else if ( limit >= 0 && count - skip > limit ) {
                finish( inputs );
                return;
            }

            if ( !output.put( row ) ) {
                finish( inputs );
                return;
            }
        }
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "sort", CollationValue.class ).map( c -> c.getFields().isEmpty() );
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return getTupleCount( inCounts.get( 0 ), settings.getInt( "limit" ), settings.getInt( "skip" ) );
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        boolean mightSort = settings.get( "sort", CollationValue.class ).map( c -> !c.getFields().isEmpty() ).orElse( true );
        boolean mightLimit = settings.get( "limit", IntValue.class ).map( i -> i.getValue() >= 0 ).orElse( true );
        boolean mightSkip = settings.get( "skip", IntValue.class ).map( i -> i.getValue() > 0 ).orElse( true );

        if ( !mightSort && (mightLimit || mightSkip) ) {
            return "Limit Rows";
        }
        if ( mightSort && !mightLimit && !mightSkip ) {
            return "Sort Rows";
        }
        return null;
    }


    public static long getTupleCount( Long inCount, int limit, int skip ) {
        long count = inCount - skip;
        if ( limit >= 0 ) {
            return Math.min( count, limit );
        }
        return count;
    }

}
