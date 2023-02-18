/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.excel;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.util.Source;

public class ExcelTranslatableTable extends ExcelTable implements QueryableTable, TranslatableTable {

    /**
     * Creates a ExcelTable.
     */
    ExcelTranslatableTable( Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource, Long tableId ) {
        super( source, protoRowType, fieldTypes, fields, excelSource, tableId );
    }


    ExcelTranslatableTable( Source source, AlgProtoDataType protoRowType, List<ExcelFieldType> fieldTypes, int[] fields, ExcelSource excelSource, Long tableId, String sheet ) {
        super( source, protoRowType, fieldTypes, fields, excelSource, tableId, sheet );
    }


    public String toString() {
        return "ExcelTranslatableTable";
    }


    /**
     * Returns an enumerable over a given projection of the fields.
     * <p>
     * Called from generated code.
     */
    public Enumerable<Object> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( excelSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                return new ExcelEnumerator<>( source, cancelFlag, fieldTypes, fields, sheet );
            }
        };
    }


    @Override
    public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
        return Schemas.tableExpression( schema, getElementType(), tableName, clazz );
    }


    @Override
    public Type getElementType() {
        return Object[].class;
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable, AlgTraitSet traitSet ) {
        // Request all fields.
        return new ExcelTableScan( context.getCluster(), algOptTable, this, fields );
    }

}
