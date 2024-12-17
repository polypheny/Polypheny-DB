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
import java.util.Optional;
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
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void reset() {

    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalCollection collection = getEntity( settings.get( COLL_KEY, EntityValue.class ) );
        DocReader reader = (DocReader) inputs.get( 0 );
        write( collection, ctx.getTransaction(), reader.getIterable(), ctx, reader.getDocCount() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return null;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LogicalCollection collection = getEntity( settings.get( COLL_KEY, EntityValue.class ) );
        write( collection, ctx.getTransaction(), inputs.get( 0 ), null, 1 );
    }


    private LogicalCollection getEntity( EntityValue setting ) throws ActivityException {
        // TODO: check if the adapter is a data store (and thus writable)
        LogicalCollection collection = setting.getCollection();
        if ( collection == null ) {
            throw new InvalidSettingException( "Specified collection does not exist", COLL_KEY );
        }
        return collection;
    }


    private void write( LogicalCollection collection, Transaction transaction, Iterable<List<PolyValue>> tuples, ExecutionContext ctx, long totalTuples ) throws Exception {
        long count = 0;
        try ( DocBatchWriter writer = new DocBatchWriter( collection, transaction ) ) {
            for ( List<PolyValue> tuple : tuples ) {
                System.out.println( "Loading value " + tuple.get( 0 ).asDocument() );
                writer.write( tuple.get( 0 ).asDocument() );

                count++;
                if ( ctx != null && count % 1024 == 0 ) {
                    ctx.updateProgress( (double) count / totalTuples );
                    ctx.checkInterrupted();
                }
            }
        }
    }

}
