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

package org.polypheny.db.adapter.parquet.document.planning;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetConvention;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.planning.ParquetPolyAlgDisplay;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyValue;

/**
 * The planner node that sets up document reading.
 * The real read starts when the generated executable plan calls
 * the entity’s scanFiltered() method
 */
@Getter
public class ParquetDocScan extends DocumentScan<ParquetDocument> implements EnumerableAlg {

    private final List<ParquetAdapterFilter<PolyValue>> filters;


    public ParquetDocScan( AlgCluster cluster, ParquetDocument entity, List<ParquetAdapterFilter<PolyValue>> filters ) {
        super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), entity );
        this.filters = List.copyOf( filters );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ParquetDocScan( getCluster(), entity, filters );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "filters", filters );
    }


    public ParquetDocScan withFilters( List<ParquetAdapterFilter<PolyValue>> filters ) {
        List<ParquetAdapterFilter<PolyValue>> combinedFilters = new ArrayList<>( this.filters );
        combinedFilters.addAll( filters );
        return new ParquetDocScan( getCluster(), entity, combinedFilters );
    }


    @Override
    public AlgDataType deriveRowType() {
        final List<AlgDataTypeField> fieldList = DocumentType.ofId().getFields();
        return getCluster().getTypeFactory().builder().add( fieldList.get( 0 ) ).build();
    }


    @Override
    public String algCompareString() {
        return super.algCompareString() + "$filters=" + filters;
    }


    @Override
    public PolyAlgArgs bindArguments() {
        int[] fields = IntStream.range( 0, entity.getTupleType().getFields().size() ).toArray();
        List<String> fieldNames = ParquetPolyAlgDisplay.fieldNames( entity, fields );
        List<String> tableFieldNames = ParquetPolyAlgDisplay.fieldNames( entity );
        return super.bindArguments()
                .put( "fields", new ListArg<>( fieldNames, StringArg::new ) )
                .put( "filters", new ListArg<>( ParquetPolyAlgDisplay.filters( filters, tableFieldNames ), StringArg::new ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double fieldRatio = ((double) entity.getParquetSource().getExportedColumns().size() + 2D) / ((double) entity.getTupleType().getFieldCount() + 2D);
        double filterRatio = filters.isEmpty() ? 1D : 0.5D;
        return super.computeSelfCost( planner, mq ).multiplyBy( ParquetConvention.COST_MULTIPLIER * fieldRatio * filterRatio );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );
        Expression runtimeFilters = EnumUtils.expressionList( filters.stream().map( ParquetAdapterFilter::toExpression ).toList() );
        // create runtime code that will call scanFiltered() on the ParquetDocument entity
        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(
                                entity.asExpression(),
                                "scanFiltered",
                                implementor.getRootExpression(),
                                runtimeFilters ) ) );
    }


    @Override
    public void register( AlgPlanner planner ) {
        planner.addRuleDuringRuntime( new ParquetDocCalcRule( AlgFactories.LOGICAL_BUILDER ) );
    }

}
