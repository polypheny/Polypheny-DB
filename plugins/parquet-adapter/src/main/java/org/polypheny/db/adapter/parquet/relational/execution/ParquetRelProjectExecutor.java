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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetFilterResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.type.entity.PolyValue;

public class ParquetRelProjectExecutor extends ParquetRelExecutor {

    public ParquetRelProjectExecutor( ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        super( table, parquetSource, fieldIndexes, schemaReader );
    }


    /**
     * Creates an enumerator for a regular parquet file(s).
     *
     * @param dataContext a data context.
     * @param fields an array of projected field indexes.
     * @param filters a list of filters. Can be empty.
     * @return {@link ParquetMultiFileEnumerator}.
     */
    public Enumerable<PolyValue[]> createEnumerator( final DataContext dataContext, final int[] fields, final List<ParquetAdapterFilter<PolyValue>> filters ) {
        registerAdapter( dataContext );
        final List<ParquetAdapterFilter<PolyValue>> resolvedFilters = ParquetFilterResolver.resolveFilters( dataContext, filters, f -> selectPhysicalBinding( table, f.columnIndex() ) );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetEnumeratorsFactory( table, fields, fieldIndexes, schemaReader, cancelFlag ).create( resolvedFilters );
            }
        };
    }

}
