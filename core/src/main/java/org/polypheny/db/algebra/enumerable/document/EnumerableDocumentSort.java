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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentSort;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;

@Slf4j
public class EnumerableDocumentSort extends DocumentSort implements EnumerableAlg {

    public EnumerableDocumentSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation ) {
        super( cluster, traits, child, collation );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        log.warn( "todo we1234123" );
        return null;
    }

}
