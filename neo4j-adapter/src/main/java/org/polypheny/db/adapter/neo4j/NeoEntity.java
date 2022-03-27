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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
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
import org.polypheny.db.adapter.neo4j.util.NeoUtil.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.NormalStatement;
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
            throw new UnsupportedOperationException();
        }


        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<Object> execute( List<NeoStatement> statements, List<PolyType> types, List<PolyType> componentTypes ) {
            Transaction trx = getTrx();

            Result res = trx.run( unwrap( statements, dataContext ) );

            Function1<Record, Object> getter = NeoQueryable.getter( types, componentTypes );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return new NeoEnumerator( res, getter );
                }
            };
        }


        private static Function1<Record, Object> getter( List<PolyType> types, List<PolyType> componentTypes ) {
            return NeoUtil.getTypesFunction( types, componentTypes );
        }


        private Transaction getTrx() {
            return namespace.transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
        }


        private String unwrap( List<NeoStatement> statements, DataContext dataContext ) {
            return statements.stream().map( s -> {
                if ( s.isPrepare() ) {
                    return s.build( dataContext.getParameterTypes(), dataContext.getParameterValues().get( 0 ) );
                }
                return ((NormalStatement) s).build();
            } ).collect( Collectors.joining( "\n " ) );
        }


        public static class NeoEnumerator implements Enumerator<Object> {

            private final Result result;
            private final Function1<Record, Object> getter;
            private Object current;


            public NeoEnumerator( Result result, Function1<Record, Object> getter ) {
                this.result = result;
                this.getter = getter;
            }


            @Override
            public Object current() {
                return current;
            }


            @Override
            public boolean moveNext() {
                if ( result.hasNext() ) {
                    this.current = getter.apply( result.next() );
                    return true;
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
