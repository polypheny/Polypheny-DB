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

package org.polypheny.db.algebra.rules;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.enumerable.EnumerableAggregate;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableFilter;
import org.polypheny.db.adapter.enumerable.EnumerableLimit;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.adapter.enumerable.EnumerableSort;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTraitSet;

@Slf4j
public class DocumentToEnumerableRule extends AlgOptRule {

    public static DocumentToEnumerableRule PROJECT_TO_ENUMERABLE = new DocumentToEnumerableRule( Type.PROJECT, operand( LogicalDocumentProject.class, any() ), "DOCUMENT_PROJECT_TO_ENUMERABLE" );
    public static DocumentToEnumerableRule AGGREGATE_TO_ENUMERABLE = new DocumentToEnumerableRule( Type.AGGREGATE, operand( LogicalDocumentAggregate.class, any() ), "DOCUMENT_AGGREGATE_TO_ENUMERABLE" );
    public static DocumentToEnumerableRule FILTER_TO_ENUMERABLE = new DocumentToEnumerableRule( Type.FILTER, operand( LogicalDocumentFilter.class, any() ), "DOCUMENT_FILTER_TO_ENUMERABLE" );
    public static DocumentToEnumerableRule SORT_TO_ENUMERABLE = new DocumentToEnumerableRule( Type.SORT, operand( LogicalDocumentSort.class, any() ), "DOCUMENT_SORT_TO_ENUMERABLE" );

    private final Type type;


    public DocumentToEnumerableRule( Type type, AlgOptRuleOperand operand, String description ) {
        super( operand, AlgFactories.LOGICAL_BUILDER, description );
        this.type = type;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        if ( type == Type.PROJECT ) {
            convertProject( call );
        } else if ( type == Type.FILTER ) {
            convertFilter( call );
        } else if ( type == Type.AGGREGATE ) {
            convertAggregate( call );
        } else if ( type == Type.SORT ) {
            convertSort( call );
        } else {
            throw new UnsupportedOperationException( "This document is not supported." );
        }

    }


    private void convertSort( AlgOptRuleCall call ) {
        LogicalDocumentSort sort = call.alg( 0 );
        AlgTraitSet out = sort.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = AlgOptRule.convert( sort.getInput(), EnumerableConvention.INSTANCE );

        AlgNode enumerable;
        if ( sort.collation.getFieldCollations().isEmpty() ) {
            enumerable = EnumerableLimit.create( input, sort.offset, sort.fetch );
        } else {
            enumerable = new EnumerableSort( sort.getCluster(), out, input, sort.collation, sort.offset, sort.fetch );
        }

        call.transformTo( enumerable );
    }


    private void convertAggregate( AlgOptRuleCall call ) {
        LogicalDocumentAggregate aggregate = call.alg( 0 );
        AlgTraitSet out = aggregate.getTraitSet().replace( EnumerableConvention.INSTANCE );

        try {
            AlgNode node = new EnumerableAggregate( aggregate.getCluster(), out, convert( aggregate.getInput(), EnumerableConvention.INSTANCE ), aggregate.indicator, aggregate.groupSet, aggregate.groupSets, aggregate.aggCalls );

            call.transformTo( node );
        } catch ( InvalidAlgException e ) {
            log.debug( e.toString() );
        }
    }


    private void convertFilter( AlgOptRuleCall call ) {
        LogicalDocumentFilter filter = call.alg( 0 );
        AlgTraitSet out = filter.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = AlgOptRule.convert( filter.getInput(), EnumerableConvention.INSTANCE );

        EnumerableFilter enumerable = new EnumerableFilter( filter.getCluster(), out, input, filter.condition );
        call.transformTo( enumerable );
    }


    private void convertProject( AlgOptRuleCall call ) {
        LogicalDocumentProject project = call.alg( 0 );
        AlgTraitSet out = project.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = AlgOptRule.convert( project.getInput(), EnumerableConvention.INSTANCE );

        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), out, input, project.projects, project.getRowType() );
        call.transformTo( enumerableProject );
    }


    private enum Type {
        PROJECT,
        FILTER,
        AGGREGATE,
        VALUES,
        SORT
    }

}
