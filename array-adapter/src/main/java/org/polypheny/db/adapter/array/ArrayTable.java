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
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.sql2rel.InitializerExpressionFactory;

/**
 * Table backed by a Java list.
 */
public class ArrayTable extends AbstractTable implements ModifiableTable, Wrapper {

    private final RelProtoDataType protoRowType;
    private final InitializerExpressionFactory initializerExpressionFactory;

    private final String physicalTableName;
    private final ArrayStore store;


    /**
     * Creates a MutableArrayTable.
     *
     * @param name                         Name of table within its schema
     * @param protoRowType                 Prototype of row type
     * @param initializerExpressionFactory How columns are populated
     * @param arrayStore
     */
    ArrayTable( String name, RelProtoDataType protoRowType, InitializerExpressionFactory initializerExpressionFactory, ArrayStore arrayStore ) {
        super();
        this.protoRowType = Objects.requireNonNull( protoRowType );
        this.initializerExpressionFactory = Objects.requireNonNull( initializerExpressionFactory );
        this.physicalTableName = name;
        this.store = arrayStore;
    }


    @Override
    public Collection getModifiableCollection() {
        return store.getRow( physicalTableName );
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
    public TableModify toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            Prepare.CatalogReader catalogReader,
            RelNode child,
            TableModify.Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        return LogicalTableModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
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
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
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
