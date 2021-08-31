/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.adapter.cottontail.rel.CottontailLimit;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.tools.RelBuilderFactory;


public class CottontailLimitRule extends CottontailConverterRule {

    CottontailLimitRule( CottontailConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Sort.class, r -> true, Convention.NONE, out, relBuilderFactory, "CottontailLimitRule" + out.getName() );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        final Sort sort = call.rel( 0 );

        // Check if this contains sort statements. The CottontailLimit only implements limits
        if ( sort.getCollation().getFieldCollations().size() > 0 ) {
            return false;
        }

        if ( sort.fetch != null ) {
            return true;
        }

        if ( sort.offset != null ) {
            return true;
        }

        return false;
    }


    @Override
    public RelNode convert( RelNode rel ) {
        Sort sort = (Sort) rel;
        final RelTraitSet traitSet = sort.getTraitSet().replace( out );
        final RelNode input;
        final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace( out );
        input = convert( sort.getInput(), inputTraitSet );

        return new CottontailLimit( sort.getCluster(), traitSet, input, sort.getCollation(), sort.offset, sort.fetch );
    }

}
