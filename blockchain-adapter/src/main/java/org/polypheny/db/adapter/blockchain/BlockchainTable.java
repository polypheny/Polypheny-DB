/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.blockchain;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.*;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.util.Pair;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockchainTable  extends AbstractTable implements QueryableTable,TranslatableTable {
    protected final String clientUrl;
    protected final RelProtoDataType protoRowType;
    private final int blocks;
    protected List<BlockchainFieldType> fieldTypes;
    protected final int[] fields;
    protected final BlockchainDataSource blockchainDataSource;
    protected final BlockchainMapper mapper;

    BlockchainTable(String clientUrl,int blocks, RelProtoDataType protoRowType, List<BlockchainFieldType> fieldTypes, int[] fields,BlockchainMapper mapper,BlockchainDataSource blockchainDataSource) {
        this.clientUrl = clientUrl;
        this.protoRowType = protoRowType;
        this.fieldTypes = fieldTypes;
        this.fields = fields;
        this.blockchainDataSource = blockchainDataSource;
        this.mapper = mapper;
        this.blocks = blocks;
    }

    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        for(RelDataTypeField field:this.protoRowType.apply(typeFactory).getFieldList()){
            types.add(field.getType());
            names.add(field.getName());
        }
        return typeFactory.createStructType( Pair.zip( names, types ) );
    }

    public String toString() {
        return "BlockchainTable";
    }


    /**
     * Returns an enumerable over a given projection of the fields.
     *
     * Called from generated code.
     */
    public Enumerable<Object[]> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter(blockchainDataSource);
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new BlockchainEnumerator<>(clientUrl,blocks, cancelFlag, true, null,mapper, new BlockchainEnumerator.ArrayRowConverter( fieldTypes, fields,true) );
            }
        };
    }


    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz ) {
        return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
    }


    @Override
    public Type getElementType() {
        return Object[].class;
    }


    @Override
    public <T> Queryable<T> asQueryable(DataContext dataContext, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable ) {
        // Request all fields.
        return new BlockchainTableScan( context.getCluster(), relOptTable, this, fields );
    }
}
