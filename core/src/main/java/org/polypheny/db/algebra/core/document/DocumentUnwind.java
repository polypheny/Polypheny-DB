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

package org.polypheny.db.algebra.core.document;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public class DocumentUnwind extends SingleAlg implements DocumentAlg {

    public String path;


    protected DocumentUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, String path ) {
        super( cluster, traits, input );

        this.path = path;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$"
                + path + "&";
    }


    @Override
    public DocType getDocType() {
        return DocType.UNWIND;
    }

}
