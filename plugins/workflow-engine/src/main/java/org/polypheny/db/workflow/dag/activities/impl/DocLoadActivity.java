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

import static org.polypheny.db.workflow.dag.activities.impl.DocLoadActivity.COLL_KEY;

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.DocBatchWriter;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;

@ActivityDefinition(type = "docLoad", displayName = "Load Collection", categories = { ActivityCategory.LOAD, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC) },
        outPorts = {})

@EntitySetting(key = COLL_KEY, displayName = "Collection", dataModel = DataModel.DOCUMENT)
@SuppressWarnings("unused")
public class DocLoadActivity implements Activity, Pipeable {

    static final String COLL_KEY = "collection";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalCollection collection = getEntity( settings.get( COLL_KEY, EntityValue.class ) );
        DocReader reader = (DocReader) inputs.get( 0 );
        write( collection, ctx.getTransaction(), reader.getIterable(), ctx, null, reader.getDocCount() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        long estimatedTupleCount = estimateTupleCount( inputs.stream().map( InputPipe::getType ).toList(), settings, ctx.getEstimatedInCounts(), ctx::getTransaction );

        LogicalCollection collection = getEntity( settings.get( COLL_KEY, EntityValue.class ) );
        write( collection, ctx.getTransaction(), inputs.get( 0 ), null, ctx, estimatedTupleCount );
    }


    private LogicalCollection getEntity( EntityValue setting ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        LogicalCollection collection = setting.getCollection();
        if ( collection == null ) {
            throw new InvalidSettingException( "Specified collection does not exist", COLL_KEY );
        }
        return collection;
    }


    private void write( LogicalCollection collection, Transaction transaction, Iterable<List<PolyValue>> tuples, ExecutionContext ctx, PipeExecutionContext pipeCtx, long totalTuples ) throws Exception {
        assert ctx != null || pipeCtx != null;

        long count = 0;
        long countDelta = Math.max( totalTuples / 100, 1 );
        try ( DocBatchWriter writer = new DocBatchWriter( collection, transaction ) ) {
            for ( List<PolyValue> tuple : tuples ) {
                writer.write( tuple.get( 0 ).asDocument() );

                count++;
                if ( count % countDelta == 0 ) {
                    double progress = (double) count / totalTuples;
                    if ( ctx != null ) {
                        ctx.updateProgress( progress );
                        ctx.checkInterrupted();
                    } else {
                        pipeCtx.updateProgress( progress );
                    }
                }
            }
        }
    }

}
