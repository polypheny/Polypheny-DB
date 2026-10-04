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

import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * This class holds instance of {@link ParquetRelExecutor} derives by their class.
 */
public class ParquetRelExecutorsFactory {

    private final ParquetRelTable table;
    private final AbstractParquetSource parquetSource;
    private final int[] fieldIndexes;
    private final ParquetSchemaReader schemaReader;
    private final Map<Class<?>, ParquetRelExecutor> executors;


    public ParquetRelExecutorsFactory( ParquetRelTable table, AbstractParquetSource parquetSource, int[] fieldIndexes, ParquetSchemaReader schemaReader ) {
        this.table = table;
        this.parquetSource = parquetSource;
        this.fieldIndexes = fieldIndexes;
        this.schemaReader = schemaReader;
        this.executors = new HashMap<>();
    }


    /**
     * Gets an instance of an executor derived from {@link ParquetRelExecutor}.
     *
     * @param executorClass an actual implementation class to retrieve.
     * @param <T> executor type.
     * @return a new or existing instance of an executor.
     */
    @SuppressWarnings("unchecked")
    public <T extends ParquetRelExecutor> T getExecutor( Class<T> executorClass ) {
        var executor = executors.get( executorClass );
        if ( executor == null ) {
            if ( executorClass.equals( ParquetRelProjectExecutor.class ) ) {
                executor = new ParquetRelProjectExecutor( table, parquetSource, fieldIndexes, schemaReader );
            } else if ( executorClass.equals( ParquetRelMetadataAggregateExecutor.class ) ) {
                executor = new ParquetRelMetadataAggregateExecutor( table, parquetSource, fieldIndexes, schemaReader );
            } else if ( executorClass.equals( ParquetRelDataAggregateExecutor.class ) ) {
                executor = new ParquetRelDataAggregateExecutor( table, parquetSource, fieldIndexes, schemaReader );
            } else if ( executorClass.equals( ParquetRelNestedJoinExecutor.class ) ) {
                executor = new ParquetRelNestedJoinExecutor( table, parquetSource, fieldIndexes, schemaReader );
            }
            if ( executor == null ) {
                throw new GenericRuntimeException( "No executor found for class " + executorClass.getName() );
            }
            executors.put( executorClass, executor );
        }
        return (T) executor;
    }

}
