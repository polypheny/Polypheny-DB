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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.logical.common.LogicalMappingTransform;
import org.polypheny.db.algebra.mapping.FieldMappingContext;
import org.polypheny.db.algebra.mapping.PhysicalMappingBinding;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.PolyAlgMetadata;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.rules.MappingTransformRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
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
        new FieldMappingContext( mapping, binding, input.getTupleType(),
                input.getCluster().getRexBuilder(), input.getCluster().getSnapshot() ).validatePhysicalNames();
        // A mapping can reorder fields, so input collations must not be copied.
        return new LogicalRelMappingTransform( input, input.getCluster().traitSetOf( Convention.NONE ), mapping, binding, targetType );
    }


    public static LogicalRelMappingTransform create( PolyAlgArgs args, List<AlgNode> inputs, AlgCluster cluster ) {
        String id = args.getArg( "mappingId", StringArg.class ).getArg();
        long mappingId;
        try {
            mappingId = Long.parseLong( id );
        } catch ( NumberFormatException e ) {
            throw MappingError.Validation.mappingId( id, e );
        }
        var snapshot = cluster.getSnapshot();
        var mapping = snapshot.mappings().getMapping( mappingId )
                .orElseThrow( () -> MappingError.Missing.mapping( mappingId ) );
        if ( inputs.size() != 1 ) {
            throw MappingError.Algebra.inputCount( inputs.size() );
        }
        AlgNode input = inputs.get( 0 );
        if ( !(input.getEntity() instanceof PhysicalEntity physical) ) {
            throw MappingError.Algebra.physicalScanRequired( mappingId );
        }
        var allocation = snapshot.alloc().getFromLogical( mapping.targetEntityId() ).stream()
                .filter( candidate -> candidate.id == physical.allocationId ).findFirst()
                .orElseThrow( () -> MappingError.Algebra.allocationMismatch( mappingId, physical.allocationId ) );
        var binding = PhysicalMappingBinding.create( mapping, allocation, Catalog.getInstance().getAdapterCatalog( allocation.adapterId )
                .orElseThrow( () -> MappingError.Missing.adapterCatalog( allocation.adapterId ) ) );
        if ( binding.physicalEntityId() != physical.id ) {
            throw MappingError.Algebra.physicalSourceMismatch( mappingId, physical.id );
        }
        return create( input, mapping, binding, allocation.getTupleType() );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        return new PolyAlgArgs( getPolyAlgDeclaration() ).put( "mappingId", new StringArg( String.valueOf( getMapping().id() ) ) );
    }


    @Override
    protected void addPolyAlgMetadata( PolyAlgMetadata metadata, AlgMetadataQuery mq ) {
        metadata.addBadge( getMapping().name(), "mapping", PolyAlgMetadata.BadgeLevel.INFO );
        var inputFields = getInput().getTupleType().getFieldNames();
        for ( var project : MappingTransformRule.REL_INSTANCE.projectFields( this ) ) {
            if ( project instanceof RexCall call && call.isA( Kind.MAPPING_CONVERT ) ) {
                var description = PolyAlgUtils.describeMappingConversion( call, inputFields );
                metadata.addBadge( description, "conversion", PolyAlgMetadata.BadgeLevel.INFO );
            }
        }
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
