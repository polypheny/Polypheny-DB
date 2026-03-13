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

package org.polypheny.db.adapter.parquet.schema;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Filterable table
 */
public class ParquetFilterableTable extends ParquetTable implements FilterableEntity {

    public ParquetFilterableTable( long id, Source source, PhysicalTable table, List<PolyType> fieldTypes, int[] fields, ParquetSource csvSource ) {
        super( id, source, table, fieldTypes, fields, csvSource );
    }

    /**
     * Pushes supported filters.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new Enumerator<>() {
                    @Override
                    public PolyValue[] current() {
                        return new PolyValue[0];
                    }
                    @Override
                    public boolean moveNext() {
                        return false;
                    }
                    @Override
                    public void reset() {

                    }
                    @Override
                    public void close() {

                    }
                };
            }
        };
    }
}

