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

import static org.polypheny.db.workflow.dag.activities.impl.LpgExtractActivity.GRAPH_KEY;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;
import org.polypheny.db.workflow.dag.settings.EntityValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;

@ActivityDefinition(type = "lpgExtract", displayName = "Extract Graph", categories = { ActivityCategory.EXTRACT, ActivityCategory.GRAPH },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.LPG) })

@EntitySetting(key = GRAPH_KEY, displayName = "Graph", dataModel = DataModel.GRAPH, mustExist = true)

@SuppressWarnings("unused")
public class LpgExtractActivity implements Activity, Pipeable {

    public static final String GRAPH_KEY = "graph";


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return LpgType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        LogicalGraph graph = settings.get( GRAPH_KEY, EntityValue.class ).getGraph();
        LpgWriter writer = ctx.createLpgWriter( 0 );
        try ( ResultIterator nodes = getResultIterator( ctx.getTransaction(), graph, false ) ) {
            for ( Iterator<PolyValue[]> it = nodes.getIterator(); it.hasNext(); ) {
                writer.writeNode( it.next()[0].asNode() );
            }
        }
        try ( ResultIterator edges = getResultIterator( ctx.getTransaction(), graph, true ) ) {
            for ( Iterator<PolyValue[]> it = edges.getIterator(); it.hasNext(); ) {
                writer.writeEdge( it.next()[0].asEdge() );
            }
        }
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        LogicalGraph graph = settings.get( GRAPH_KEY, EntityValue.class ).getGraph();
        Transaction transaction = transactionSupplier.get();

        long nodeCount = getCount( graph, "MATCH (n) RETURN COUNT(n)", transaction );
        if ( nodeCount < 0 ) {
            return -1;
        }
        long edgeCount = getCount( graph, "MATCH ()-[r]->() RETURN COUNT(r)", transaction );
        if ( edgeCount < 0 ) {
            return -1;
        }
        return nodeCount + edgeCount;
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        LogicalGraph graph = settings.get( GRAPH_KEY, EntityValue.class ).getGraph();
        try ( ResultIterator nodes = getResultIterator( ctx.getTransaction(), graph, false ) ) {
            for ( Iterator<List<PolyValue>> it = CheckpointReader.arrayToListIterator( nodes.getIterator(), true ); it.hasNext(); ) {
                if ( !output.put( it.next() ) ) {
                    return;
                }
            }
        }
        try ( ResultIterator edges = getResultIterator( ctx.getTransaction(), graph, true ) ) {
            for ( Iterator<List<PolyValue>> it = CheckpointReader.arrayToListIterator( edges.getIterator(), true ); it.hasNext(); ) {
                if ( !output.put( it.next() ) ) {
                    return;
                }
            }
        }
    }


    private ResultIterator getResultIterator( Transaction transaction, LogicalGraph graph, boolean isEdges ) {
        String query = isEdges ? "MATCH ()-[r]->() RETURN r" : "MATCH (n) RETURN n";

        System.out.println( "Before exec (isEdges: " + isEdges + ")" );
        long start = System.currentTimeMillis();
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery( query, "cypher", graph.namespaceId, transaction );
        System.out.println( "After exec (isEdges: " + isEdges + ", " + (System.currentTimeMillis() - start) + " ms)" );
        return executedContext.getIterator();
    }


    private long getCount( LogicalGraph graph, String countQuery, Transaction transaction ) {
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                countQuery, "cypher", graph.getNamespaceId(), transaction );
        try ( ResultIterator resultIterator = executedContext.getIterator() ) {
            return resultIterator.getIterator().next()[0].asNumber().longValue();
        } catch ( NoSuchElementException | IndexOutOfBoundsException | NullPointerException ignored ) {
            return -1;
        }
    }

}
