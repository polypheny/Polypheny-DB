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

package org.polypheny.db.adapter.enumerable;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Converter;
import org.polypheny.db.algebra.logical.LogicalConverter;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;

public class EnumerableConverterRule extends ConverterRule {

    public EnumerableConverterRule( AlgBuilderFactory algBuilderFactory ) {
        super( LogicalConverter.class, Converter::isScan, Convention.NONE, EnumerableConvention.INSTANCE, algBuilderFactory, "EnumerableConverter" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        Converter converter = (Converter) alg;
        return converter.getConvertedScan();
        /*AlgNode orig = convert( converter.getOriginal(), converter.getOriginal().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final EnumerableConvention out = EnumerableConvention.INSTANCE;
        final AlgTraitSet traitSet = converter.getTraitSet().replace( out );
        return new EnumerableConverter( converter.getCluster(), traitSet, orig )*/
    }

}
