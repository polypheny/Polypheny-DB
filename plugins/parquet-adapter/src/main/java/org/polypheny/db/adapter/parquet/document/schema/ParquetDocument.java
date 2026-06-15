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

package org.polypheny.db.adapter.parquet.document.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocAggregateExecutor;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocEnumerator;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocScan;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetMultiFileEnumerator;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTableBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;

/**
 * Physical collection wrapper for the document model.
 * Represents one Parquet-backed collection inside Polypheny
 */
@Getter
public class ParquetDocument extends PhysicalCollection implements ScannableEntity, TranslatableEntity {

    private final List<ParquetSourceFile> sourceFiles;
    private final Map<String, List<String>> columnPaths;
    private final AbstractParquetSource parquetSource;


    public ParquetDocument( PhysicalCollection collection, DiscoveredTableBinding binding, AbstractParquetSource parquetSource ) {
        super(
                collection.id,
                collection.allocationId,
                collection.logicalId,
                collection.namespaceId,
                collection.name,
                collection.namespaceName,
                collection.adapterId );
        this.sourceFiles = List.copyOf( binding.sourceFiles() ); // add multi-file handling
        this.columnPaths = Map.copyOf( binding.columnPaths() );
        this.parquetSource = parquetSource;
    }


    /**
     * Build xpression tree:
     * - get the adapter catalog from parquetSource
     * - ask it for the physical entity with this document’s id
     * - cast the result to ParquetDocument
     *
     * @return Expression
     */
    @Override
    public Expression asExpression() {
        Expression argExp = Expressions.constant( this.id );
        return Expressions.convert_(
                Expressions.call(
                        Expressions.call( this.parquetSource.asExpression(), "getAdapterCatalog" ),
                        "getPhysical",
                        argExp ),
                ParquetDocument.class );
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        return scanFiltered( dataContext, List.of() );
    }


    /**
     * creates enumerable with resolve filters
     *
     * @param dataContext context
     * @param filters - parquet filters
     * @return ParquetDocEnumerator
     */
    public Enumerable<PolyValue[]> scanFiltered( DataContext dataContext, List<ParquetAdapterFilter<PolyValue>> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final List<ParquetAdapterFilter<PolyValue>> resolvedFilters = filters.stream().map( filter -> resolveFilter( dataContext, filter ) ).toList();
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                FiltersContainer filtersContainer = FiltersContainer.shared( resolvedFilters );
                // handle multi-files
                return new ParquetMultiFileEnumerator(
                        sourceFiles,
                        sourceFile -> {
                            ParquetSourceReader reader = new ParquetSourceReader(
                                    sourceFile.asSource(),
                                    cancelFlag,
                                    null,
                                    filtersContainer.nativeFilters() );
                            return new ParquetDocEnumerator( reader, filtersContainer );
                        } );
            }
        };
    }


    /**
     * Creates a data enumerator that should go though the parquet file.
     *
     * @param dataContext a Polypheny data context.
     * @param fields an array of field indexes.
     * @param filters a list of filters to be applied.
     * @param groupFields an array of group field indexes.
     * @param aggregateKinds an array of aggregate functions.
     * @param aggregateArgs an array of aggregate functions arguments indexes.
     * @return an enumerator
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> dataAggregate( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        return new ParquetDocAggregateExecutor( sourceFiles, exportedColumns(), columnPaths )
                .createDataEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Creates a metadata enumerator that reads only parquet file statistics, no data is being read.
     *
     * @param dataContext a Polypheny data context.
     * @param fields an array of field indexes.
     * @param filters a list of filters to be applied.
     * @param groupFields an array of group field indexes.
     * @param aggregateKinds an array of aggregate functions.
     * @param aggregateArgs an array of aggregate functions arguments indexes.
     * @return an enumerator.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> metadataAggregate( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        return new ParquetDocAggregateExecutor( sourceFiles, exportedColumns(), columnPaths )
                .createMetadataEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Checks whether all provided aggregate functions are supported by data aggregates. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsDataAggregate( int[] fields, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return new ParquetDocAggregateExecutor( sourceFiles, exportedColumns(), columnPaths )
                .supportsDataAggregate( fields, groupSet, aggregateCalls );
    }


    /**
     * Checks whether all provided aggregate functions are supported by metadata aggregates. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsMetadataAggregate( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return new ParquetDocAggregateExecutor( sourceFiles, exportedColumns(), columnPaths )
                .supportsMetadataAggregate( fields, filters, groupSet, aggregateCalls );
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new ParquetDocScan( cluster, this, List.of() );
    }


    @Override
    public AlgDataType getTupleType() {
        return getTupleType( AlgDataTypeFactory.DEFAULT );
    }


    @Override
    public AlgDataType getTupleType( AlgDataTypeFactory typeFactory ) {
        return buildDocumentType( typeFactory );
    }


    private AlgDataType buildDocumentType( AlgDataTypeFactory typeFactory ) {
        List<ExportedColumn> columns = exportedColumns();
        if ( columns == null || columns.isEmpty() ) {
            return DocumentType.ofId();
        }

        List<AlgDataTypeField> fields = new ArrayList<>();
        for ( ExportedColumn column : columns ) {
            AlgDataType type = typeFactory.createPolyType( column.type() );
            if ( column.nullable() ) {
                type = typeFactory.createTypeWithNullability( type, true );
            }
            fields.add( new AlgDataTypeFieldImpl( -1L, column.name(), column.physicalPosition(), type ) );
        }
        return new DocumentType( fields );
    }


    private List<ExportedColumn> exportedColumns() {
        return parquetSource.getExportedColumns().getOrDefault( name, List.of() );
    }


    /**
     * add dynamic parameters to parquet filter if needed
     *
     * @param dataContext context
     * @param filter parquet filter
     * @return ParquetFilter
     */
    private ParquetAdapterFilter<PolyValue> resolveFilter( DataContext dataContext, ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical( filter.operator(), filter.operands().stream()
                    .map( operand -> resolveFilter( dataContext, operand ) )
                    .toList() );
        }
        if ( filter.dynamicParamIndex() == null ) {
            return filter;
        }
        return new ParquetAdapterFilter<>(
                filter.columnIndex(),
                filter.operator(),
                dataContext.getParameterValue( filter.dynamicParamIndex() ) );
    }

}
