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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.document.LogicalDocumentUnwind;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

public class EnumerableDocumentUnwindRule extends ConverterRule {


    public EnumerableDocumentUnwindRule() {
        super( LogicalDocumentUnwind.class, Convention.NONE, EnumerableConvention.INSTANCE, EnumerableDocumentUnwindRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalDocumentUnwind unwind = (LogicalDocumentUnwind) alg;
        AlgTraitSet out = unwind.getTraitSet().replace( EnumerableConvention.INSTANCE );

        return new EnumerableDocumentUnwind( unwind.getCluster(), out, AlgOptRule.convert( unwind.getInput(), out ), unwind.path );
    }

}
