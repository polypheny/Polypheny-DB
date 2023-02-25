/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.polypheny.db.adapter.cottontail.CottontailEntity;
import org.polypheny.db.adapter.cottontail.algebra.CottontailTableModify;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.UnsupportedFromInsertShuttle;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.ModifiableEntity;
import org.polypheny.db.tools.AlgBuilderFactory;


public class CottontailTableModificationRule extends CottontailConverterRule {

    CottontailTableModificationRule( CottontailConvention out, AlgBuilderFactory algBuilderFactory ) {
        super( RelModify.class, CottontailTableModificationRule::supports, Convention.NONE, out, algBuilderFactory, "CottontailTableModificationRule:" + out.getName() );
    }


    private static boolean supports( RelModify modify ) {
        return !modify.isInsert() || !UnsupportedFromInsertShuttle.contains( modify );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final RelModify modify = call.alg( 0 );
        if ( modify.getEntity().unwrap( CottontailEntity.class ) == null ) {
            return false;
        }

        if ( !modify.getEntity().unwrap( CottontailEntity.class ).getUnderlyingConvention().equals( this.out ) ) {
            return false;
        }
        return modify.getOperation() != Modify.Operation.MERGE;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final RelModify modify = (RelModify) alg;

        final ModifiableEntity modifiableTable = modify.getEntity().unwrap( ModifiableEntity.class );

        if ( modifiableTable == null ) {
            return null;
        }
        if ( modify.getEntity().unwrap( CottontailEntity.class ) == null ) {
            return null;
        }

        final AlgTraitSet traitSet = modify.getTraitSet().replace( out );

        return new CottontailTableModify(
                modify.getCluster(),
                traitSet,
                modify.getEntity(),
                modify.getCatalogReader(),
                AlgOptRule.convert( modify.getInput(), traitSet ),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened()
        );
    }

}
