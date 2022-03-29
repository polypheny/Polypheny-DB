/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.neo4j.rules.NeoScan;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.LogicalModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class NeoEntity extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    public final String phsicalEntityName;
    public final long id;
    public final AlgProtoDataType rowType;


    protected NeoEntity( String physicalEntityName, AlgProtoDataType proto, long id ) {
        super( Object[].class );
        this.phsicalEntityName = physicalEntityName;
        this.rowType = proto;
        this.id = id;
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new NeoQueryable<>( dataContext, schema, this, tableName );
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return rowType.apply( getTypeFactory() );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        final AlgOptCluster cluster = context.getCluster();
        return new NeoScan( cluster, cluster.traitSetOf( NeoConvention.INSTANCE ), algOptTable, this );
    }


    @Override
    public Collection<?> getModifiableCollection() {
        throw new UnsupportedOperationException( "getModifiableCollection is not supported by the NEO4j adapter." );
    }


    @Override
    public Modify toModificationAlg( AlgOptCluster cluster, AlgOptTable table, CatalogReader catalogReader, AlgNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
    }


    public static class NeoQueryable<T> extends AbstractTableQueryable<T> {

        @Getter
        private final NeoEntity entity;
        private final NeoNamespace namespace;


        public NeoQueryable( DataContext dataContext, SchemaPlus schema, QueryableTable table, String tableName ) {
            super( dataContext, schema, table, tableName );
            this.entity = (NeoEntity) table;
            this.namespace = schema.unwrap( NeoNamespace.class );
        }


        @Override
        public Enumerator<T> enumerator() {
            return execute( String.format( "MATCH (n:%s) RETURN n", entity.phsicalEntityName ), List.of(), List.of(), Map.of() ).enumerator();
        }


        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<T> execute( String query, List<PolyType> types, List<PolyType> componentTypes, Map<Long, Pair<PolyType, PolyType>> prepared ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( namespace.store );

            List<Result> results = new ArrayList<>();
            if ( dataContext.getParameterValues().size() == 1 ) {
                results.add( trx.run( query, toParameters( dataContext.getParameterValues().get( 0 ), prepared ) ) );
            } else if ( dataContext.getParameterValues().size() > 0 ) {
                for ( Map<Long, Object> value : dataContext.getParameterValues() ) {
                    results.add( trx.run( query, toParameters( value, prepared ) ) );
                }
            } else {
                results.add( trx.run( query ) );
            }

            Function1<Record, T> getter = NeoQueryable.getter( types, componentTypes );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<T> enumerator() {
                    return new NeoEnumerator<>( results, getter );
                }
            };
        }


        private Map<String, Object> toParameters( Map<Long, Object> values, Map<Long, Pair<PolyType, PolyType>> parameterTypes ) {
            Map<String, Object> parameters = new HashMap<>();
            for ( Entry<Long, Object> entry : values.entrySet() ) {
                parameters.put( NeoUtil.asParameter( entry.getKey(), false ), NeoUtil.fixParameterValue( entry.getValue(), parameterTypes.get( entry.getKey() ) ) );
            }
            return parameters;
        }


        private static <T> Function1<Record, T> getter( List<PolyType> types, List<PolyType> componentTypes ) {
            //noinspection unchecked
            return (Function1<Record, T>) NeoUtil.getTypesFunction( types, componentTypes );
        }


        private Transaction getTrx() {
            return namespace.transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
        }


        public static class NeoEnumerator<T> implements Enumerator<T> {

            private final List<Result> results;
            private final Function1<Record, T> getter;
            private Result result;
            private T current;
            private int pos = 0;


            public NeoEnumerator( List<Result> results, Function1<Record, T> getter ) {
                this.results = results;
                this.result = results.get( pos );
                pos++;
                this.getter = getter;
            }


            @Override
            public T current() {
                return current;
            }


            @Override
            public boolean moveNext() {
                if ( result.hasNext() ) {
                    this.current = getter.apply( result.next() );
                    return true;
                }
                if ( results.size() > pos ) {
                    this.result = results.get( pos );
                    pos++;

                    return moveNext();
                }

                return false;
            }


            @Override
            public void reset() {
                throw new UnsupportedOperationException();
            }


            @Override
            public void close() {
                this.result.consume();
            }


        }

    }

}
