package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Relational scan algebra node for Parquet-backed physical tables
 */
@Getter
public class ParquetRelScan extends RelScan<ParquetRelTable> implements EnumerableAlg {

    private final int[] fields;
    private final List<ParquetAdapterFilter> filters;


    public ParquetRelScan( AlgCluster cluster, ParquetRelTable table, int[] fields ) {
        this( cluster, table, fields, List.of() );
    }


    public ParquetRelScan( AlgCluster cluster, ParquetRelTable table, int[] fields, List<ParquetAdapterFilter> filters ) {
        super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), table );
        this.fields = fields;
        this.filters = List.copyOf( filters );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ParquetRelScan( getCluster(), entity, fields, filters );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "fields", Primitive.asList( fields ) ).item( "filters", filters );
    }


    @Override
    public String algCompareString() {
        return super.algCompareString() + "$fields=" + Primitive.asList( fields ) + "$filters=" + filters;
    }


    @Override
    public AlgDataType deriveRowType() {
        final List<AlgDataTypeField> fieldList = entity.getTupleType().getFields();
        final AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for ( int field : fields ) {
            builder.add( fieldList.get( field ) );
        }
        return builder.build();
    }


    @Override
    public void register( AlgPlanner planner ) {
        registerRules( planner );
    }


    public static void registerRules( AlgPlanner planner ) {
        planner.addRuleDuringRuntime( ParquetRelTableScanRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetRelScanRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetEnumerableFilterScanRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetEnumerableCalcScanRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetRelJoinRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetEnumerableJoinRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetEnumerableLimitJoinRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetEnumerableFilterJoinRule.INSTANCE );
        planner.addRuleDuringRuntime( ParquetEnumerableCalcJoinRule.INSTANCE );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double fieldRatio = ((double) fields.length + 2D) / ((double) entity.getTupleType().getFieldCount() + 2D);
        double filterRatio = filters.isEmpty() ? 1D : 0.5D;
        return super.computeSelfCost( planner, mq ).multiplyBy( fieldRatio * filterRatio );
    }


    public ParquetRelScan withFilters( List<ParquetAdapterFilter> filters ) {
        List<ParquetAdapterFilter> combinedFilters = new ArrayList<>( this.filters );
        combinedFilters.addAll( filters );
        return new ParquetRelScan( getCluster(), entity, fields, combinedFilters );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );
        Expression runtimeFilters = EnumUtils.expressionList( filters.stream().map( ParquetAdapterFilter::toExpression ).toList() );
        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(
                                entity.asExpression( ParquetRelTable.class ),
                                "project",
                                implementor.getRootExpression(),
                                Expressions.constant( fields ),
                                runtimeFilters ) ) );
    }

}
