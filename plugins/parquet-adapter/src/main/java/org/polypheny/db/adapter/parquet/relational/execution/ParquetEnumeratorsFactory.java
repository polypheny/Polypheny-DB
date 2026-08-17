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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.parquet.relational.filter.ParquetMultiFilterEvaluator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetDataAggregateExecutor;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Creates projected row enumerators with the common file pruning and reader selection logic.
 */
public final class ParquetEnumeratorsFactory {

    private final ParquetRelTable table;
    private final int[] fields;
    private final int[] allFields;
    private final ParquetSchemaReader schemaReader;
    private final AtomicBoolean cancelFlag;
    private final ParquetMultiFilterEvaluator<ParquetSourceFile> sourceFileEvaluator;


    public ParquetEnumeratorsFactory( ParquetRelTable table, int[] fields, int[] allFields, ParquetSchemaReader schemaReader, AtomicBoolean cancelFlag ) {
        this.table = table;
        this.fields = Arrays.copyOf( fields, fields.length );
        this.allFields = Arrays.copyOf( allFields, allFields.length );
        this.schemaReader = schemaReader;
        this.cancelFlag = cancelFlag;
        this.sourceFileEvaluator = ParquetDataAggregateExecutor.createParquetSourceFileEvaluatorsChain( f -> ParquetRelExecutor.selectPhysicalBinding( table, f.columnIndex() ) );
    }


    /**
     * Creates {@link ParquetMultiFileEnumerator} enumerator with the provided list of filters.
     *
     * @param filters a list of filters to apply on the source files.
     * @return an enumerator.
     */
    public Enumerator<PolyValue[]> create( List<ParquetAdapterFilter<PolyValue>> filters ) {
        return new ParquetMultiFileEnumerator(
                table.getBinding().sourceFiles(),
                ( sourceFile, residualFilters ) -> create( sourceFile, residualFilters ),
                sourceFileEvaluator,
                filters );
    }


    /**
     * Creates an enumerator based on the source file type.
     * - source file contains nested repeated fields -> {@link ParquetNestedRepeatedRelEnumerator}
     * - source file contains nested but not repeated fields -> {@link ParquetNestedNonRepeatedRelEnumerator}
     * - source file doesn't contain nested fields -> {@link ParquetRelEnumerator}
     *
     * @param sourceFile a source file to be read.
     * @return a new enumerator that best match the source file.
     */
    public Enumerator<PolyValue[]> create( FilterableParquetSourceFile sourceFile ) {
        return create( sourceFile.file(), sourceFile.filters() );
    }


    /**
     * Creates an enumerator based on the source file type.
     * - source file contains nested repeated fields -> {@link ParquetNestedRepeatedRelEnumerator}
     * - source file contains nested but not repeated fields -> {@link ParquetNestedNonRepeatedRelEnumerator}
     * - source file doesn't contain nested fields -> {@link ParquetRelEnumerator}
     *
     * @param sourceFile a source file to be read.
     * @param filters a list of filters to be applied.
     * @return a new enumerator that best match the source file.
     */
    private Enumerator<PolyValue[]> create( ParquetSourceFile sourceFile, List<ParquetAdapterFilter<PolyValue>> filters ) {
        return ParquetRelExecutor.enumeratorForFile( table, sourceFile, fields, allFields, schemaReader, cancelFlag, FiltersContainer.shared( filters ) );
    }


}
