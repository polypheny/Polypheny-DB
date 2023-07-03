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

package org.polypheny.db.algebra.enumerable.document;

import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.relational.LogicalSort;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;

public class DocumentSortToSortRule extends AlgOptRule {

    public static final DocumentSortToSortRule INSTANCE = new DocumentSortToSortRule();


    private DocumentSortToSortRule() {
        super( operand( LogicalDocumentSort.class, any() ), DocumentSortToSortRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalDocumentSort sort = call.alg( 0 );
        call.transformTo( LogicalSort.create( sort.getInput(), sort.fieldExps, sort.collation, sort.offset, sort.fetch ) );
    }

}
