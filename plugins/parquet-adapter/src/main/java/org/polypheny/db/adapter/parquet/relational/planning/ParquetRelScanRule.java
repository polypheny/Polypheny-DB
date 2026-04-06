package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.List;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilderFactory;


public class ParquetRelScanRule extends AlgOptRule {

    public static final ParquetRelScanRule INSTANCE = new ParquetRelScanRule( AlgFactories.LOGICAL_BUILDER );


    public ParquetRelScanRule(AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( LogicalRelProject.class, operand( ParquetRelScan.class, none() ) ),
                algBuilderFactory,
                ParquetRelScanRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalRelProject project = call.alg( 0 );
        final ParquetRelScan scan = call.alg( 1 );
        int[] fields = getProjectFields( project.getProjects() );
        if ( fields == null ) {
            return;
        }
        call.transformTo( new ParquetRelScan( scan.getCluster(), scan.getEntity(), fields ) );
    }


    private int[] getProjectFields( List<RexNode> exps ) {
        final int[] fields = new int[exps.size()];
        for ( int i = 0; i < exps.size(); i++ ) {
            final RexNode exp = exps.get( i );
            if ( exp instanceof RexIndexRef ) {
                fields[i] = ((RexIndexRef) exp).getIndex();
            } else {
                return null;
            }
        }
        return fields;
    }

}
