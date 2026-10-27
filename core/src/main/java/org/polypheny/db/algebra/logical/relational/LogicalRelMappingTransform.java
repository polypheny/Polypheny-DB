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

package org.polypheny.db.algebra.logical.relational;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.logical.common.LogicalMappingTransform;
import org.polypheny.db.algebra.mapping.FieldMappingContext;
import org.polypheny.db.algebra.mapping.PhysicalMappingBinding;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.trait.ModelTrait;


/**
 * Relational physical-to-logical schema mapping for a selected allocation.
 */
@Getter
public final class LogicalRelMappingTransform extends LogicalMappingTransform implements RelAlg {

    private final PhysicalMappingBinding binding;


    private LogicalRelMappingTransform( AlgNode input, AlgTraitSet traits, SchemaMapping mapping, PhysicalMappingBinding binding, AlgDataType targetType ) {
        super( input, traits.replace( ModelTrait.RELATIONAL ), mapping, targetType );
        binding.validate( mapping );
        this.binding = binding;
    }


    public static LogicalRelMappingTransform create( AlgNode input, SchemaMapping mapping, PhysicalMappingBinding binding, AlgDataType targetType ) {
        new FieldMappingContext( mapping, binding, input.getTupleType(), input.getCluster().getRexBuilder() ).validatePhysicalNames();
        // A mapping can reorder fields, so input collations must not be copied.
        return new LogicalRelMappingTransform( input, input.getCluster().traitSetOf( Convention.NONE ), mapping, binding, targetType );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalRelMappingTransform( sole( inputs ), traitSet, getMapping(), binding, rowType );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        return super.explainTerms( writer ).item( "binding", binding );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$" + getMapping() + "$" + binding + "$"
                + rowType.getFullTypeString() + "$" + rowType.getFieldIds() + "$" + input.algCompareString() + "&";
    }

}
