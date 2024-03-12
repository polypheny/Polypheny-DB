/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.adapter.cottontail.algebra.CottontailSort;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Matches
 */
public class CottontailSortRule extends CottontailConverterRule {

    CottontailSortRule( AlgBuilderFactory algBuilderFactory ) {
        super( Sort.class, r -> true, Convention.NONE, CottontailConvention.INSTANCE, algBuilderFactory, "CottontailSortRule" );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        return !sort.getCollation().getFieldCollations().isEmpty() || sort.fetch != null || sort.offset != null;
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        Sort sort = (Sort) alg;
        if ( containsDynamicProject( alg ) ) {
            return null;
        }

        final AlgTraitSet traitSet = sort.getTraitSet().replace( out );
        final AlgNode input;
        final AlgTraitSet inputTraitSet = sort.getInput().getTraitSet().replace( out );
        input = convert( sort.getInput(), inputTraitSet );

        return new CottontailSort( sort.getCluster(), traitSet, input, sort.getCollation(), sort.offset, sort.fetch );
    }


    private boolean containsDynamicProject( AlgNode alg ) {
        // Check if the input of the sort is a project that contains a dynamic parameter (i.e., a parameter that is not known to cottontail and cannot be pushed down)
        return alg.getInput( 0 ) instanceof AlgSubset subset
                && subset.getOriginal() instanceof Project project
                && project.getProjects().stream().anyMatch( p -> p instanceof RexDynamicParam );
    }

}
