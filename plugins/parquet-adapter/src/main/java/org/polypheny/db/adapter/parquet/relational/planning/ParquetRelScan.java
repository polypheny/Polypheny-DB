package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.relational.RelScan;
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


    public ParquetRelScan(AlgCluster cluster, ParquetRelTable table, int[] fields ) {
        super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), table );
        this.fields = fields;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ParquetRelScan( getCluster(), entity, fields );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "fields", Primitive.asList( fields ) );
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
        planner.addRule( ParquetRelScanRule.INSTANCE );
        planner.addRule( ParquetRelJoinRule.INSTANCE );
        planner.addRule( ParquetEnumerableJoinRule.INSTANCE );
        planner.addRule( ParquetEnumerableFilterJoinRule.INSTANCE );
        planner.addRule( ParquetEnumerableCalcJoinRule.INSTANCE );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( ((double) fields.length + 2D) / ((double) entity.getTupleType().getFieldCount() + 2D) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );
        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(
                                entity.asExpression( ParquetRelTable.class ),
                                "project",
                                implementor.getRootExpression(),
                                Expressions.constant( fields ) ) ) );
    }

}
