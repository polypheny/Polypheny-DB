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

import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;

@SuperBuilder(toBuilder = true)
public abstract class DocumentScan<E extends Entity> extends Scan<E> implements DocumentAlg {


    /**
     * Creates a {@link DocumentScan}.
     * {@link ModelTrait#DOCUMENT} node, which scans the content of a collection.
     */
    public DocumentScan( AlgCluster cluster, AlgTraitSet traitSet, E collection ) {
        super( cluster, traitSet.replace( ModelTrait.DOCUMENT ), collection );
        this.rowType = DocumentType.ofId();
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "table", entity.id )
                .item( "name", entity.name )
                .item( "layer", entity.getLayer() );
    }


    @Override
    public String algCompareString() {
        // need the name for cross-model queries
        return getClass().getSimpleName() + "$"
                + entity.id + "$"
                + entity.name + "$"
                + entity.getLayer() + "&";
    }


    @Override
    public DocType getDocType() {
        return DocType.SCAN;
    }

}
