/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail.rules;


import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.CottontailTable;
import org.polypheny.db.adapter.cottontail.algebra.CottontailTableModify;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.tools.AlgBuilderFactory;


public class CottontailTableModificationRule extends CottontailConverterRule {

    CottontailTableModificationRule( CottontailConvention out, AlgBuilderFactory algBuilderFactory ) {
        super( Modify.class, r -> true, Convention.NONE, out, algBuilderFactory, "CottontailTableModificationRule:" + out.getName() );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final Modify modify = call.alg( 0 );
        if ( modify.getTable().unwrap( CottontailTable.class ) == null ) {
            return false;
        }

        if ( !modify.getTable().unwrap( CottontailTable.class ).getUnderlyingConvention().equals( this.out ) ) {
            return false;
        }
        return modify.getOperation() != Operation.MERGE;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Modify modify = (Modify) alg;

        final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );

        if ( modifiableTable == null ) {
            return null;
        }
        if ( modify.getTable().unwrap( CottontailTable.class ) == null ) {
            return null;
        }

        final AlgTraitSet traitSet = modify.getTraitSet().replace( out );

        return new CottontailTableModify(
                modify.getCluster(),
                traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                AlgOptRule.convert( modify.getInput(), traitSet ),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened()
        );
    }

}
