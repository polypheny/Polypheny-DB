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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.planning.ParquetPolyAlgDisplay;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Parquet-convention scan for relational Parquet tables.
 */
@Getter
public class ParquetRelScan extends RelScan<ParquetRelTable> implements ParquetAlg {

    private final int[] fields;
    private final List<ParquetAdapterFilter<PolyValue>> filters;


    public ParquetRelScan(AlgCluster cluster, ParquetRelTable table, int[] fields) {
        this(cluster, table, fields, List.of());
    }


    public ParquetRelScan(AlgCluster cluster, ParquetRelTable table, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters) {
        super(cluster, cluster.traitSetOf(ParquetRelConvention.INSTANCE).replace(ModelTrait.RELATIONAL), table);
        this.fields = fields;
        this.filters = List.copyOf(filters);
    }


    @Override
    public AlgNode copy(AlgTraitSet traitSet, List<AlgNode> inputs) {
        assert inputs.isEmpty();
        return new ParquetRelScan(getCluster(), entity, fields, filters);
    }


    @Override
    public AlgWriter explainTerms(AlgWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(fields)).item("filters", filters);
    }


    @Override
    public String algCompareString() {
        return super.algCompareString() + "$fields=" + Primitive.asList(fields) + "$filters=" + filters;
    }


    @Override
    public PolyAlgArgs bindArguments() {
        List<String> fieldNames = ParquetPolyAlgDisplay.fieldNames(entity, fields);
        List<String> tableFieldNames = ParquetPolyAlgDisplay.fieldNames(entity);
        return super.bindArguments()
                .put("fields", new ListArg<>(fieldNames, StringArg::new))
                .put("filters", new ListArg<>(ParquetPolyAlgDisplay.filters(filters, tableFieldNames), StringArg::new));
    }


    @Override
    public AlgDataType deriveRowType() {
        final List<AlgDataTypeField> fieldList = entity.getTupleType().getFields();
        final AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }


    @Override
    public void register(AlgPlanner planner) {
        ParquetRelConvention.INSTANCE.register(planner);
    }


    @Override
    public AlgOptCost computeSelfCost(AlgPlanner planner, AlgMetadataQuery mq) {
        double fieldRatio = ((double) fields.length + 2D) / ((double) entity.getTupleType().getFieldCount() + 2D);
        double filterRatio = filters.isEmpty() ? 1D : 0.5D;
        return super.computeSelfCost(planner, mq).multiplyBy(ParquetRelConvention.COST_MULTIPLIER * fieldRatio * filterRatio);
    }


    public ParquetRelScan withFilters(List<ParquetAdapterFilter<PolyValue>> filters) {
        List<ParquetAdapterFilter<PolyValue>> combinedFilters = new ArrayList<>(this.filters);
        combinedFilters.addAll(filters);
        return new ParquetRelScan(getCluster(), entity, fields, combinedFilters);
    }


    public ParquetRelScan withFields(int[] fields) {
        return new ParquetRelScan(getCluster(), entity, fields, filters);
    }

    /**
     * The function defines the expression call: function to call when node invoked
     *
     * @param implementor original implementor
     * @return a call expression
     */
    @Override
    public Expression implement(EnumerableAlgImplementor implementor) {
        Expression runtimeFilters = EnumUtils.expressionList(filters.stream().map(ParquetAdapterFilter::toExpression).toList());
        return Expressions.call(
                entity.asExpression(ParquetRelTable.class), // class
                "project", // function to call
                implementor.getRootExpression(), // context - parameter 1
                Expressions.constant(fields), // projection - parameter 2
                runtimeFilters); // filters - parameter 3
    }

}
