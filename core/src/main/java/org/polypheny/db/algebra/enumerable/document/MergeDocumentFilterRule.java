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

package org.polypheny.db.algebra.enumerable.document;

import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;

public class MergeDocumentFilterRule extends AlgOptRule {

    public static final MergeDocumentFilterRule INSTANCE = new MergeDocumentFilterRule();


    private MergeDocumentFilterRule() {
        super( operand( DocumentFilter.class, operand( DocumentFilter.class, any() ) ) );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalDocumentFilter projectParent = call.alg( 0 );
        LogicalDocumentFilter projectChild = call.alg( 1 );

        AlgBuilder builder = call.builder();
        builder.clear();
        builder.push( projectChild.getInput() );
        builder.documentFilter( builder.getRexBuilder().makeCall( OperatorRegistry.get( OperatorName.AND ), projectChild.condition, projectParent.condition ) );
        LogicalDocumentFilter newFilter = (LogicalDocumentFilter) builder.build();
        call.transformTo( newFilter );
    }

}
