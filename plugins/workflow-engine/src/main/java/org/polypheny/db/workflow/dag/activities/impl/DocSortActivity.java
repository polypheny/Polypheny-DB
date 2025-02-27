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

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
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

@ActivityDefinition(type = "docSort", displayName = "Sort / Limit Documents", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The collection sorted by the specified fields and possibly limited number of documents.") },
        shortDescription = "This activity can be used to both sort a collection and limit its number of documents. Note that some adapters do not keep track of the document order.",
        longDescription = """
                This activity can be used to both sort a collection and limit its number of documents.
                
                > # Note
                >
                > Some adapters do not keep track of the sort order.
                > This implies that the order only persists until the output is materialized in a checkpoint.
                > If a downstream activity expects sorted input data, consider enabling activity fusion or pipelining. This removes checkpoints between compatible activities.
                """
)
@CollationSetting(key = "sort", displayName = "Sort Fields", shortDescription = "Specify the field(s) to sort the documents by. If no field is selected, the original order is used.", allowRegex = false)
@IntSetting(key = "limit", displayName = "Document Limit", defaultValue = -1, min = -1, shortDescription = "The total number of documents to include, or -1 to include all documents.")
@IntSetting(key = "skip", displayName = "Skip Documents", defaultValue = 0, min = 0, shortDescription = "The number of documents to skip, or 0 to start at the first document.")
@SuppressWarnings("unused")
public class DocSortActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ).isPresent() ) {
            return inTypes.get( 0 ).asOutTypes();
        }
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgDataType type = inputs.get( 0 ).getTupleType();
        Pair<AlgCollation, List<RexNode>> pair = settings.get( "sort", CollationValue.class ).toAlgCollation( 0 );
        int limit = settings.getInt( "limit" );
        int skip = settings.getInt( "skip" );

        if ( pair.left.getFieldCollations().isEmpty() && limit == -1 && skip == 0 ) {
            ctx.logInfo( "Detected trivial sort" );
            return inputs.get( 0 ); // trivial sort
        }
        RexLiteral limitLiteral = limit == -1 ? null : ActivityUtils.getRexLiteral( limit );
        RexLiteral skipLiteral = skip == 0 ? null : ActivityUtils.getRexLiteral( skip );

        return LogicalDocumentSort.create( inputs.get( 0 ), pair.left, pair.right, skipLiteral, limitLiteral );
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
        for ( List<PolyValue> tuple : inputs.get( 0 ) ) {
            count++;
            if ( count <= skip ) {
                continue;
            } else if ( limit >= 0 && count - skip > limit ) {
                finish( inputs );
                return;
            }

            if ( !output.put( tuple ) ) {
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
        return RelSortActivity.getTupleCount( inCounts.get( 0 ), settings.getInt( "limit" ), settings.getInt( "skip" ) );
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        boolean mightSort = settings.get( "sort", CollationValue.class ).map( c -> !c.getFields().isEmpty() ).orElse( true );
        boolean mightLimit = settings.get( "limit", IntValue.class ).map( i -> i.getValue() >= 0 ).orElse( true );
        boolean mightSkip = settings.get( "skip", IntValue.class ).map( i -> i.getValue() > 0 ).orElse( true );

        if ( !mightSort && (mightLimit || mightSkip) ) {
            return "Limit Documents";
        }
        if ( mightSort && !mightLimit && !mightSkip ) {
            return "Sort Documents";
        }
        return null;
    }

}
