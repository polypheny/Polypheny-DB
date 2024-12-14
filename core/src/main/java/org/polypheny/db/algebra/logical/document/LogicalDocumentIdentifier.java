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

package org.polypheny.db.algebra.logical.document;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Identifier;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;

public class LogicalDocumentIdentifier extends Identifier implements DocumentAlg {


    protected LogicalDocumentIdentifier( AlgCluster cluster, AlgTraitSet traits, Entity entity, AlgNode input ) {
        super( cluster, traits, entity, input );
    }


    public static LogicalDocumentIdentifier create( Entity entity, AlgNode input ) {
        return new LogicalDocumentIdentifier( input.getCluster(), input.getTraitSet(), entity, input );
    }


    @Override
    public DocType getDocType() {
        //ToDo TH: Is this the proper type for this?
        return DocType.PROJECT;
    }

}
