/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.execution;

import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetAggregateSource;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetMetadataAggregateExecutor;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;

/**
 * Relational entry point for metadata aggregate execution.
 */
public class ParquetRelMetadataAggregateExecutor extends ParquetRelExecutor {

    public ParquetRelMetadataAggregateExecutor( ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        super( table, parquetSource, fieldIndexes, schemaReader );
    }


    public Enumerable<PolyValue[]> createEnumerator( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        registerAdapter( dataContext );
        return new ParquetMetadataAggregateExecutor( aggregateSource() )
                .createEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Checks whether all provided aggregate functions are supported by metadata aggregates. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsMetadataAggregate( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return new ParquetMetadataAggregateExecutor( aggregateSource() )
                .supportsMetadataAggregate( fields, filters, groupSet, aggregateCalls );
    }


    private ParquetAggregateSource aggregateSource() {
        return new ParquetAggregateSource() {
            @Override
            public List<ParquetSourceFile> sourceFiles() {
                return table.getBinding().sourceFiles();
            }


            @Override
            public ParquetSchemaReader schemaReader() {
                return schemaReader;
            }


            @Override
            public int fieldCount() {
                return table.columns.size();
            }


            @Override
            public PolyType fieldType( int field ) {
                return table.columns.get( field ).type;
            }


            @Override
            public ParquetColumnBinding binding( int field ) {
                return ParquetRelExecutor.selectPhysicalBinding( table, field );
            }
        };
    }

}
