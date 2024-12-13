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

package org.polypheny.db.adapter.neo4j;

import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.NeoProcedureProvider;
import org.polypheny.db.adapter.neo4j.Neo4jPlugin.Neo4jStore;
import org.polypheny.db.adapter.neo4j.NeoGraph.NeoQueryable;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgCall;
import org.polypheny.db.adapter.neo4j.types.NestedPolyType;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NeoProcedureCallHandler implements QueryableEntity {

    final long adapterId;
    final long callNodeId;
    final NeoLpgCall callNode;
    final Neo4jStore store;

    public NeoProcedureCallHandler( long adapterId, long callNodeId ) {
        this.adapterId = adapterId;
        this.callNodeId = callNodeId;
        store = (Neo4jStore) AdapterManager.getInstance().getStore( adapterId ).orElseThrow();
        callNode = store.getCallNode( callNodeId );
    }

    public static Expression getHandlerAsExpression( long adapterId, long callNodeId ) {
        return Expressions.new_( NeoProcedureCallHandler.class, Expressions.constant( adapterId ), Expressions.constant( callNodeId ) );
    }

    public static Expression asExpression( long adapterId, long callNodeId ) {
        return Expressions.call(
                Expressions.convert_(
                        getHandlerAsExpression( adapterId, callNodeId ),
                        NeoProcedureCallHandler.class ),
                "asQueryable",
                DataContext.ROOT,
                Catalog.SNAPSHOT_EXPRESSION );
    }


    @Override
    public Queryable<PolyValue[]> asQueryable( DataContext dataContext, Snapshot snapshot ) {
        return new NeoProcedureCallable( dataContext,  snapshot, adapterId );
    }


    @Override
    public AlgDataType getTupleType( AlgDataTypeFactory typeFactory ) {
        return callNode.getTupleType();
    }


    public static class NeoProcedureCallable extends AbstractQueryable<PolyValue[]> {

        private final DataContext dataContext;
        //private final NeoProcedureCallHandler handler;
        private final long adapterId;
        private final Neo4jStore store;
        private final TransactionProvider transactionProvider;

        public NeoProcedureCallable(DataContext dataContext, Snapshot snapshot, long adapterId) {
            this.dataContext = dataContext;
            this.adapterId = adapterId;
            store = (Neo4jStore) AdapterManager.getInstance().getAdapter( adapterId ).orElseThrow();
            this.transactionProvider = store.getTransactionProvider();
        }

        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> execute( String query, NestedPolyType types ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( store );

            List<Result> results = new ArrayList<>();
            results.add( trx.run( query ) );

            Function1<Record, PolyValue[]> getter = NeoQueryable.getter( types );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<PolyValue[]> enumerator() {
                    return new NeoEnumerator( results, getter );
                }
            };
        }

        /**
         * This method returns the functions, which transforms a given record into the corresponding object representation.
         *
         * @param types the types for which function is created
         * @return the function, which transforms the {@link Record}
         */
        static <T> Function1<Record, T> getter( NestedPolyType types ) {
            //noinspection unchecked
            return (Function1<Record, T>) NeoUtil.getTypesFunction( types );
        }


        /**
         * Gets an already open transaction of creates a new one if it not exists.
         */
        private Transaction getTrx() {
            return transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
        }


        /**
         * Commit the transaction.
         */
        private void commit() {
            transactionProvider.commit( dataContext.getStatement().getTransaction().getXid() );
        }


        @Override
        public Type getElementType() {
            return Object[].class;
        }


        @Override
        public Expression getExpression() {
            return null;
        }


        @Override
        public QueryProvider getProvider() {
            return dataContext.getQueryProvider();
        }


        @Override
        public @NotNull Iterator<PolyValue[]> iterator() {
            return Linq4j.enumeratorIterator( enumerator() );
        }


        @Override
        public Enumerator<PolyValue[]> enumerator() {
            return null;
        }

    }

}
