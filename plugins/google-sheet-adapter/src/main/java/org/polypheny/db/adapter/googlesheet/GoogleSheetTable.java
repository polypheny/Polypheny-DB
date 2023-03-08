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

package org.polypheny.db.adapter.googlesheet;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.util.Pair;


/**
 * Base table class based on individual Google Sheets.
 */
public class GoogleSheetTable extends AbstractTable implements QueryableTable, TranslatableTable {

    protected final URL sheetsUrl;
    protected final int querySize;
    protected final String tableName;
    protected final AlgProtoDataType protoRowType;
    protected final int[] fields;
    protected final GoogleSheetSource googleSheetSource;
    protected List<GoogleSheetFieldType> fieldTypes;


    public GoogleSheetTable(
            URL sheetsUrl,
            int querySize,
            String tableName,
            AlgProtoDataType protoRowType,
            int[] fields,
            GoogleSheetSource googleSheetSource,
            List<GoogleSheetFieldType> fieldTypes ) {
        this.sheetsUrl = sheetsUrl;
        this.querySize = querySize;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.fields = fields;
        this.googleSheetSource = googleSheetSource;
        this.fieldTypes = fieldTypes;
    }


    public String toString() {
        return "GoogleSheetTable";
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        final List<AlgDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        for ( AlgDataTypeField field : this.protoRowType.apply( typeFactory ).getFieldList() ) {
            types.add( field.getType() );
            names.add( field.getName() );
        }
        return typeFactory.createStructType( Pair.zip( names, types ) );
    }


    /**
     * Returns an enumerator over a given projection
     */
    public Enumerable<Object> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( googleSheetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                return new GoogleSheetEnumerator<>( sheetsUrl, querySize, tableName, cancelFlag, fieldTypes, fields, googleSheetSource );
            }
        };
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        throw new UnsupportedOperationException();
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
    public AlgNode toAlg( AlgOptTable.ToAlgContext context, AlgOptTable algOptTable, AlgTraitSet traitSet ) {
        // Request all fields.
        return new GoogleSheetTableScanProject( context.getCluster(), algOptTable, this, fields );
    }

}
