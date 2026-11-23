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

package org.polypheny.db.algebra.logical.document;


import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.logical.common.LogicalMappingTransform;
import org.polypheny.db.algebra.mapping.DocumentMappingBinding;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.trait.ModelTrait;

/**
 * LogicalDocumentMappingTransform is the query plan node that says to apply a document mapping to the results of a physical scan.
 */
@Getter
public final class LogicalDocumentMappingTransform extends LogicalMappingTransform implements DocumentAlg {

    private final DocumentMappingBinding binding;


    private LogicalDocumentMappingTransform( AlgNode input, AlgTraitSet traits, SchemaMapping mapping, DocumentMappingBinding binding ) {
        super( input, traits.replace( ModelTrait.DOCUMENT ), mapping, input.getTupleType() );
        binding.validate( mapping );
        this.binding = binding;
    }


    public static LogicalDocumentMappingTransform create( AlgNode input, SchemaMapping mapping, DocumentMappingBinding binding ) {
        if ( input.getModel() != DataModel.DOCUMENT ) {
            throw MappingError.Unsupported.documentMappingRequired( mapping.id() );
        }
        return new LogicalDocumentMappingTransform( input, input.getCluster().traitSetOf( Convention.NONE ), mapping, binding );
    }


    public static LogicalDocumentMappingTransform create( PolyAlgArgs args, List<AlgNode> inputs, AlgCluster cluster ) {
        String id = args.getArg( "mappingId", StringArg.class ).getArg();
        long mappingId;
        try {
            mappingId = Long.parseLong( id );
        } catch ( NumberFormatException e ) {
            throw MappingError.Validation.mappingId( id, e );
        }
        if ( inputs.size() != 1 ) {
            throw MappingError.Algebra.inputCount( inputs.size() );
        }
        var mapping = cluster.getSnapshot().mappings().getMapping( mappingId )
                .orElseThrow( () -> MappingError.Missing.mapping( mappingId ) );
        var input = inputs.get( 0 );
        if ( !(input.getEntity() instanceof PhysicalEntity physical) ) {
            throw MappingError.Algebra.physicalScanRequired( mappingId );
        }
        var allocation = cluster.getSnapshot().alloc().getFromLogical( mapping.targetEntityId() ).stream()
                .filter( a -> a.id == physical.allocationId ).findFirst()
                .orElseThrow( () -> MappingError.Algebra.allocationMismatch( mappingId, physical.allocationId ) );
        var binding = DocumentMappingBinding.create( mapping, allocation,
                Catalog.getInstance().getAdapterCatalog( allocation.adapterId ).orElseThrow( () -> MappingError.Missing.adapterCatalog( allocation.adapterId ) ) );
        if ( binding.physicalEntityId() != physical.id ) {
            throw MappingError.Algebra.physicalSourceMismatch( mappingId, physical.id );
        }
        return create( input, mapping, binding );
    }


    @Override
    public DocType getDocType() {
        return DocType.MAPPING;
    }


    @Override
    public PolyAlgArgs bindArguments() {
        return new PolyAlgArgs( getPolyAlgDeclaration() ).put( "mappingId", new StringArg( String.valueOf( getMapping().id() ) ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
        return new LogicalDocumentMappingTransform( sole( inputs ), traits, getMapping(), binding );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        return super.explainTerms( writer ).item( "binding", binding );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$" + getMapping() + "$" + binding + "$" + input.algCompareString() + "&";
    }

}
