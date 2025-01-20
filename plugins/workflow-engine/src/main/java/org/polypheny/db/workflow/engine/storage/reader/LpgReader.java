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

package org.polypheny.db.workflow.engine.storage.reader;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.Triple;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.workflow.engine.storage.QueryUtils;

public class LpgReader extends CheckpointReader {

    public static final int PREVIEW_NODES_LIMIT = 100;


    public LpgReader( LogicalGraph graph, Transaction transaction ) {
        super( graph, transaction );
    }


    public long getNodeCount() {
        return getCount( "MATCH (n) RETURN COUNT(n)" );
    }


    public long getEdgeCount() {
        return getCount( "MATCH ()-[r]->() RETURN COUNT(r)" );
    }


    @Override
    public AlgNode getAlgNode( AlgCluster cluster ) {
        AlgTraitSet traits = AlgTraitSet.createEmpty().plus( ModelTrait.GRAPH );
        return new LogicalLpgScan( cluster, traits, entity, entity.getTupleType() );
    }


    public Iterator<PolyNode> getNodeIterator() {
        Iterator<PolyValue[]> it = executeCypherQuery( "MATCH (n) RETURN n" );
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }


            @Override
            public PolyNode next() {
                return it.next()[0].asNode();
            }
        };
    }


    public Iterable<PolyNode> getNodeIterable() {
        return this::getNodeIterator;
    }


    public Iterator<PolyEdge> getEdgeIterator() {
        Iterator<PolyValue[]> it = executeCypherQuery( "MATCH ()-[r]->() RETURN r" );
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }


            @Override
            public PolyEdge next() {
                return it.next()[0].asEdge();
            }
        };
    }


    public Iterable<PolyEdge> getEdgeIterable() {
        return this::getEdgeIterator;
    }


    @Override
    public Iterator<PolyValue[]> getArrayIterator() {
        return new Iterator<>() {
            private final Iterator<PolyNode> nodeIterator = getNodeIterator();
            private Iterator<PolyEdge> edgeIterator = null;


            @Override
            public boolean hasNext() {
                if ( nodeIterator.hasNext() ) {
                    return true;
                }

                if ( edgeIterator == null ) {
                    edgeIterator = getEdgeIterator();
                }
                return edgeIterator.hasNext();
            }


            @Override
            public PolyValue[] next() {
                if ( edgeIterator == null ) {
                    return new PolyValue[]{ nodeIterator.next() };
                }
                return new PolyValue[]{ edgeIterator.next() };
            }
        };
    }


    @Override
    public long getTupleCount() {
        return getNodeCount() + getEdgeCount();
    }


    @Override
    public DataModel getDataModel() {
        return DataModel.GRAPH;
    }


    @Override
    public Triple<Result<?, ?>, Integer, Long> getPreview() {
        LogicalGraph graph = getGraph();
        String query = "MATCH (n) RETURN n LIMIT " + PREVIEW_NODES_LIMIT;
        UIRequest request = UIRequest.builder()
                .namespace( Catalog.snapshot().getNamespace( graph.getNamespaceId() ).orElseThrow().getName() )
                .build();
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "cypher", graph.getNamespaceId(), transaction );

        return Triple.of( LanguageCrud.getGraphResult( executedContext, request, executedContext.getStatement() ).build(),
                PREVIEW_NODES_LIMIT,
                getNodeCount() );
    }


    private Iterator<PolyValue[]> executeCypherQuery( String query ) {
        ExecutedContext executedContext = QueryUtils.parseAndExecuteQuery(
                query, "cypher", getGraph().getNamespaceId(), transaction );
        Iterator<PolyValue[]> it = executedContext.getIterator().getIterator();
        registerIterator( it );
        return it;
    }


    private long getCount( String countQuery ) {
        Iterator<PolyValue[]> it = executeCypherQuery( countQuery );
        try {
            return it.next()[0].asLong().longValue();
        } catch ( NoSuchElementException | IndexOutOfBoundsException | NullPointerException ignored ) {
            return 0;
        }
    }


    private LogicalGraph getGraph() {
        return (LogicalGraph) entity;
    }

}
