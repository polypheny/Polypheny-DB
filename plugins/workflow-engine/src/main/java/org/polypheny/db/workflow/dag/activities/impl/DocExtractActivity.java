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

import static org.polypheny.db.workflow.dag.activities.impl.DocExtractActivity.COLL_KEY;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "docExtract", displayName = "Extract Collection", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC, description = "The extracted collection of documents.") },
        shortDescription = "Extracts an existing collection from Polypheny and outputs its documents.")

@EntitySetting(key = COLL_KEY, displayName = "Collection", dataModel = DataModel.DOCUMENT, mustExist = true,
        shortDescription = "The collection to extract, identified by namespace and name.")

@SuppressWarnings("unused")
public class DocExtractActivity implements Activity, Fusable, Pipeable {

    public static final String COLL_KEY = "collection";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalCollection collection = settings.get( COLL_KEY, EntityValue.class ).getCollection();
        DocWriter writer = ctx.createDocWriter( 0 );
        try ( ResultIterator result = getResultIterator( ctx.getTransaction(), collection ) ) { // transaction will get committed or rolled back externally
            for ( Iterator<PolyValue[]> it = result.getIterator(); it.hasNext(); ) {
                writer.write( it.next()[0].asDocument() );
            }
        }

    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        LogicalCollection collection = settings.get( COLL_KEY, EntityValue.class ).getCollection();
        AlgTraitSet traits = cluster.traitSetOf( ModelTrait.DOCUMENT );
        return new LogicalDocumentScan( cluster, traits, collection );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        LogicalCollection collection = settings.get( COLL_KEY, EntityValue.class ).getCollection();
        String query = "db.\"" + collection.getName() + "\".count({})";

        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "MQL", collection.getNamespaceId(), transactionSupplier.get() );

        try ( ResultIterator resultIterator = executedContext.getIterator() ) {
            try {
                PolyDocument doc = resultIterator.getIterator().next()[0].asDocument();
                return doc.get( PolyString.of( "count" ) ).asNumber().longValue();
            } catch ( NoSuchElementException | IndexOutOfBoundsException | NullPointerException ignored ) {
                return -1;
            }
        }
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LogicalCollection collection = settings.get( COLL_KEY, EntityValue.class ).getCollection();
        try ( ResultIterator result = getResultIterator( ctx.getTransaction(), collection ) ) { // transaction will get committed or rolled back externally
            Iterator<List<PolyValue>> it = CheckpointReader.arrayToListIterator( result.getIterator(), true );
            while ( it.hasNext() ) {
                if ( !output.put( it.next() ) ) {
                    break;
                }
            }
        }
    }


    private ResultIterator getResultIterator( Transaction transaction, LogicalCollection collection ) {
        String query = "db.\"" + collection.getName() + "\".find({})";

        System.out.println( "Before exec" );
        long start = System.currentTimeMillis();
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery( query, "MQL", collection.getNamespaceId(), transaction );
        System.out.println( "After exec (" + (System.currentTimeMillis() - start) + " ms)" );
        return executedContext.getIterator();
    }

}
