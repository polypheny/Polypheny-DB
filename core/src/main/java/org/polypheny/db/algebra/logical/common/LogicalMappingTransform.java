/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.algebra.logical.common;

import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Applies a schema mapping to an allocation's input.
 * Field rules are translated into expressions
 * by MappingTransformRule before physical implementation.
 */
@Getter
public abstract class LogicalMappingTransform extends SingleAlg {

    private final SchemaMapping mapping;


    protected LogicalMappingTransform( AlgNode input, AlgTraitSet traits, SchemaMapping mapping, AlgDataType targetType ) {
        super( input.getCluster(), traits.replace( Convention.NONE ), input );
        this.mapping = mapping;
        this.rowType = targetType;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        return super.explainTerms( writer )
                .item( "mapping", mapping )
                .item( "targetType", rowType )
                .item( "targetColumns", rowType.getFieldIds() );
    }


    @Override
    public boolean isDataModifying() {
        return false;
    }

}
