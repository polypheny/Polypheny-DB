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
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocEnumerator;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocScan;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.algebra.AlgNode;
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
import org.polypheny.db.util.Source;

/**
 * Physical collection wrapper for the document model.
 * Represents one Parquet-backed collection inside Polypheny
 */
public class ParquetDocument extends PhysicalCollection implements ScannableEntity, TranslatableEntity {

    private final Source source;
    @Getter
    private final AbstractParquetSource parquetSource;


    public ParquetDocument( PhysicalCollection collection, Source source, AbstractParquetSource parquetSource ) {
        super(
                collection.id,
                collection.allocationId,
                collection.logicalId,
                collection.namespaceId,
                collection.name,
                collection.namespaceName,
                collection.adapterId );
        this.source = source;
        this.parquetSource = parquetSource;
    }


    /**
     * Build xpression tree:
     * - get the adapter catalog from parquetSource
     * - ask it for the physical entity with this document’s id
     * - cast the result to ParquetDocument
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
     * @param dataContext context
     * @param filters - parquet filters
     * @return ParquetDocEnumerator
     */
    public Enumerable<PolyValue[]> scanFiltered( DataContext dataContext, List<ParquetAdapterFilter> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final List<ParquetAdapterFilter> resolvedFilters = filters.stream().map( filter -> resolveFilter( dataContext, filter ) ).toList();
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetDocEnumerator( source, cancelFlag, resolvedFilters );
            }
        };
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
        List<ExportedColumn> columns = parquetSource.getExportedColumns().get( name );
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


    /**
     * add dynamic parameters to parquet filter if needed
     * @param dataContext context
     * @param filter parquet filter
     * @return ParquetFilter
     */
    private ParquetAdapterFilter resolveFilter( DataContext dataContext, ParquetAdapterFilter filter ) {
        if ( filter.dynamicParamIndex() == null ) {
            return filter;
        }
        return new ParquetAdapterFilter(
                filter.columnIndex(),
                filter.operator(),
                dataContext.getParameterValue( filter.dynamicParamIndex() ) );
    }

}
