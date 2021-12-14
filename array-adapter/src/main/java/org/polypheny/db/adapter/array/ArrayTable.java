/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.array;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.util.InitializerExpressionFactory;

/**
 * Table backed by a Java list.
 */
public class ArrayTable extends AbstractTable implements ModifiableTable, Wrapper {

    private final AlgProtoDataType protoRowType;
    private final InitializerExpressionFactory initializerExpressionFactory;

    private final String physicalTableName;
    private final ArrayStore store;


    /**
     * Creates a MutableArrayTable.
     *
     * @param name Name of table within its schema
     * @param protoRowType Prototype of row type
     * @param initializerExpressionFactory How columns are populated
     * @param arrayStore
     */
    ArrayTable( String name, AlgProtoDataType protoRowType, InitializerExpressionFactory initializerExpressionFactory, ArrayStore arrayStore ) {
        super();
        this.protoRowType = Objects.requireNonNull( protoRowType );
        this.initializerExpressionFactory = Objects.requireNonNull( initializerExpressionFactory );
        this.physicalTableName = name;
        this.store = arrayStore;
    }


    @Override
    public Collection<Object[]> getModifiableCollection() {
        return store.getRow( physicalTableName );
    }


    @Override
    public TableModify toModificationAlg( AlgOptCluster cluster, AlgOptTable table, CatalogReader catalogReader, AlgNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        return LogicalTableModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new AbstractTableQueryable<T>( dataContext, schema, this, tableName ) {
            @Override
            public Enumerator<T> enumerator() {
                //noinspection unchecked
                return (Enumerator<T>) Linq4j.enumerator( store.getRow( physicalTableName ) );
            }
        };
    }


    @Override
    public Type getElementType() {
        return Object[].class;
    }


    @Override
    public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
        return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        if ( aClass.isInstance( initializerExpressionFactory ) ) {
            return aClass.cast( initializerExpressionFactory );
        }
        return super.unwrap( aClass );
    }
}
