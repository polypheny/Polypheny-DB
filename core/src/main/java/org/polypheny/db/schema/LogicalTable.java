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

package org.polypheny.db.schema;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;

public class LogicalTable extends AbstractQueryableTable implements TranslatableTable, ScannableTable, ModifiableTable {

    private AlgProtoDataType protoRowType;

    @Getter
    private final SchemaType schemaType;

    @Getter
    private final String logicalSchemaName;
    @Getter
    private final String logicalTableName;

    @Getter
    private final List<Long> columnIds;
    @Getter
    private final List<String> logicalColumnNames;

    protected LogicalTable(
            Long tableId,
            String logicalSchemaName,
            String logicalTableName,
            List<Long> columnIds,
            List<String> logicalColumnNames,
            AlgProtoDataType protoRowType,
            SchemaType schemaType ) {
        super( Object[].class );
        this.tableId = tableId;
        this.logicalSchemaName = logicalSchemaName;
        this.logicalTableName = logicalTableName;
        this.columnIds = columnIds;
        this.logicalColumnNames = logicalColumnNames;
        this.protoRowType = protoRowType;
        this.schemaType = schemaType;
    }


    public String toString() {
        return "LogicTable {" + logicalSchemaName + "." + logicalTableName + "}";
    }


    @Override
    public TableModify toModificationAlg(
            AlgOptCluster cluster,
            AlgOptTable table,
            CatalogReader catalogReader,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        return new LogicalTableModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                input,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public Collection getModifiableCollection() {
        throw new RuntimeException( "getModifiableCollection() is not implemented for Logical Tables!" );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        throw new RuntimeException( "asQueryable() is not implemented for Logical Tables!" );
    }


    @Override
    public Enumerable<Object[]> scan( DataContext root ) {
        throw new RuntimeException( "scan() is not implemented for Logical Tables!" );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        throw new RuntimeException( "toAlg() is not implemented for Logical Tables!" );
    }


}
