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

package org.polypheny.db.webui.crud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager.PhysicalSource;
import org.polypheny.db.catalog.exceptions.MappingConflictException;
import org.polypheny.db.catalog.exceptions.MappingDuplicateException;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.exceptions.MappingException;
import org.polypheny.db.catalog.exceptions.MappingIncompatibleException;
import org.polypheny.db.catalog.exceptions.MappingMissingException;
import org.polypheny.db.catalog.exceptions.MappingRevisionException;
import org.polypheny.db.catalog.exceptions.MappingUnsupportedException;
import org.polypheny.db.catalog.exceptions.MappingValidationException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.mapping.DocumentMappingState;
import org.polypheny.db.catalog.mapping.RelMappingState;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.mapping.MappingModel;
import org.polypheny.db.webui.models.requests.MappingRequest.RelCreateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.DetailRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.DocumentCreateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.DocumentDraftRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.DocumentUpdateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.ListRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.MappingIdRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.OverviewRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.RemoveRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.RelDraftRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.RelUpdateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.RelValidationRequest;

/**
HTTP boundary for existing, allocation-backed mappings.
 */
@Slf4j
public class MappingCrud {

    private static final ObjectMapper REQUEST_MAPPER = new ObjectMapper()
            .enable( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES )
            .enable( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES )
            .disable( DeserializationFeature.ACCEPT_FLOAT_AS_INT )
            .enable( DeserializationFeature.FAIL_ON_TRAILING_TOKENS );
    private final Crud crud;


    public MappingCrud( Crud crud ) {
        this.crud = crud;
    }

    /**
     * Returns existing, saved mappings for the mapping-list screens
     */
    public void getMappings( Context context ) {
        respond( context, () -> {
            OverviewRequest request = read( context, OverviewRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                validateAdapterFilter( catalog, request.adapterId() );
                var snapshot = catalog.getSnapshot();
                List<DataModel> adapterSourceModels = request.adapterId() == null ? List.of() : AdapterManager.getInstance().getSource( request.adapterId() )
                        .map( source -> source.getSupportedDataModels().stream().sorted().toList() ).orElseGet( List::of );
                var mappings = snapshot.mappings().getMappings().stream()
                        .filter( mapping -> request.targetEntityId() == null || mapping.targetEntityId() == request.targetEntityId() )
                        .filter( mapping -> request.adapterId() == null || SchemaMappingManager.sourceAllocations( catalog, mapping ).stream()
                                .anyMatch( allocation -> allocation.adapterId == request.adapterId() ) )
                        .map( mapping -> summary( catalog, mapping.id() ) )
                        .sorted( Comparator.comparing( MappingModel.Summary::mappingName ).thenComparingLong( MappingModel.Summary::mappingId ) ).toList();
                return new MappingModel.OverviewResponse( mappings, adapterSourceModels );
            }
        } );
    }

    /**
     * Loads an existing mapping by its ID and returns its full details for the view or edit screen
     */
    public void getMapping( Context context ) {
        respond( context, () -> {
            var request = read( context, MappingIdRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                return detail( catalog, request.mappingId(), null );
            }
        } );
    }

    /**
     * Returns the available sources for creating a mapping, optionally filtering by adapter and excluding sources already mapped to a specified target.
     */
    public void getRelMappingSources( Context context ) {
        respond( context, () -> {
            var request = read( context, OverviewRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                validateAdapterFilter( catalog, request.adapterId() );
                var snapshot = catalog.getSnapshot();
                var seenSources = new HashSet<>();
                var sources = snapshot.rel().getTables( (Pattern) null, null ).stream()
                        .filter( table -> table.entityType == EntityType.SOURCE && !table.modifiable )
                        .sorted( Comparator.comparingLong( table -> table.id ) )
                        .flatMap( table -> snapshot.alloc().getFromLogical( table.id ).stream()
                        .filter( allocation -> request.adapterId() == null || allocation.adapterId == request.adapterId() ) )
                        .distinct().map( source -> RelMappingState.draft( catalog, source.adapterId, source.logicalId, null, source.id ) )
                        .filter( RelMappingState::editable )
                        .filter( state -> request.targetEntityId() == null || !SchemaMappingManager.isPhysicalSourceMappedToTarget( catalog, state.mapping(), request.targetEntityId() ) )
                        .filter( state -> seenSources.add( SchemaMappingManager.physicalSources( catalog, state.mapping() ) ) )
                        .map( MappingModel.RelSource::from ).toList();
                return new MappingModel.RelSourcesResponse( sources );
            }
        } );
    }


    public void getDocumentMappingSources( Context context ) {
        respond( context, () -> {
            var request = read( context, OverviewRequest.class );
            var catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                var snapshot = catalog.getSnapshot();
                if ( request.targetEntityId() != null ) {
                    var target = snapshot.doc().getCollection( request.targetEntityId() ).orElseThrow( MappingError.Missing::resource );
                    if ( !DocumentMappingState.supportsExistingTarget( snapshot, target ) ) {
                        return new MappingModel.DocumentSourcesResponse( List.of() );
                    }
                }
                if ( request.adapterId() != null && snapshot.getAdapter( request.adapterId() ).filter( a -> a.type == AdapterType.SOURCE ).isEmpty() ) {
                    throw MappingError.Missing.resource();
                }
                var seen = new HashSet<PhysicalSource>();
                var sources = snapshot.doc().getCollections( null, null ).stream()
                        .filter( c -> c.entityType == EntityType.SOURCE && !c.modifiable )
                        .sorted( Comparator.comparingLong( c -> c.id ) )
                        .flatMap( c -> snapshot.alloc().getFromLogical( c.id ).stream() )
                        .filter( a -> request.adapterId() == null || request.adapterId() == a.adapterId )
                        .filter( a -> snapshot.getAdapter( a.adapterId ).filter( adapter -> adapter.type == AdapterType.SOURCE ).isPresent() )
                        .map( a -> DocumentMappingState.draft( catalog, a.adapterId, a.logicalId, a.id ) )
                        .filter( DocumentMappingState::editable )
                        .filter( s -> request.targetEntityId() == null
                                || !SchemaMappingManager.isPhysicalSourceMappedToTarget( catalog, s.mapping(), request.targetEntityId() ) )
                        .filter( s -> seen.add( new PhysicalSource( s.adapter().id, s.physical().namespaceName, s.physical().name ) ) )
                        .map( s -> new MappingModel.DocumentSource( s.adapter().id, s.adapter().uniqueName, s.mapping().targetEntityId(), s.physical().allocationId,
                                s.physical().namespaceName, s.physical().name ) ).toList();
                return new MappingModel.DocumentSourcesResponse( sources );
            }
        } );
    }


    public void getDocumentMappingDraft( Context context ) {
        respond( context, () -> {
            var request = read( context, DocumentDraftRequest.class );
            var catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                var state = DocumentMappingState.draft( catalog, request.adapterId(), request.sourceEntityId(), request.sourceAllocationId(), request.existingTargetEntityId() );
                if ( !state.editable() ) {
                    throw MappingError.Unsupported.editor( state.readOnlyReason() );
                }
                return MappingModel.DocumentDetail.from( state );
            }
        } );
    }


    public void createDocumentMapping( Context context ) {
        respond( context, () -> {
            var request = read( context, DocumentCreateRequest.class );
            return transact( transaction -> MappingModel.DocumentDetail.from( DdlManager.getInstance()
                    .createDocumentMapping( request.sourceEntityId(), request.toCommand(), transaction.createStatement() ) ) );
        } );
    }


    public void updateDocumentMapping( Context context ) {
        respond( context, () -> {
            var request = read( context, DocumentUpdateRequest.class );
            return transact( transaction -> MappingModel.DocumentDetail.from( DdlManager.getInstance()
                    .updateDocumentMapping( request.toCommand(), transaction.createStatement() ) ) );
        } );
    }


    /**
     * Prepares a proposed mapping for the Create Mapping screen
     */
    public void getRelMappingDraft( Context context ) {
        respond( context, () -> {
            var request = read( context, RelDraftRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                var state = RelMappingState.draft( catalog, request.adapterId(), request.targetEntityId(), request.existingTargetEntityId(), request.sourceAllocationId() );
                if ( !state.editable() ) {
                    throw MappingError.Unsupported.editor( state.readOnlyReason() );
                }
                return MappingModel.RelDetail.from( state, catalog );
            }
        } );
    }


    /**
     * Validates the submitted mapping draft and creates a saved mapping,
     * either creating a new target table or connecting the selected source to an existing target
     */
    public void createRelMapping( Context context ) {
        respond( context, () -> {
            var request = read( context, RelCreateRequest.class );
            return transact( transaction -> MappingModel.RelDetail.from( DdlManager.getInstance()
                    .createRelMapping( request.targetEntityId(), request.toCommand(), transaction.createStatement() ), Catalog.getInstance() ) );
        } );
    }


    /**
     * Remove existing mapping
     */
    public void removeMapping( Context context ) {
        respond( context, () -> {
            var request = read( context, RemoveRequest.class );
            return transact( transaction -> {
                DdlManager.getInstance().removeMapping( request.mappingId(), request.expectedRevision(), transaction.createStatement() );
                return Map.of( "removed", true );
            } );
        } );
    }


    /**
     * Checks that an optional adapter ID is non negative and exists in the catalog
     */
    private static void validateAdapterFilter( Catalog catalog, Long adapterId ) {
        if ( adapterId != null && (adapterId < 0 || catalog.getSnapshot().getAdapter( adapterId ).isEmpty()) ) {
            throw MappingError.Missing.resource();
        }
    }


    /**
     * Supplies the list of mappings for each adapter.
     * Mappings appear in the left sidebar under the adapter name
     */
    public void getAdapterMappings( Context context ) {
        respond( context, () -> {
            ListRequest request = read( context, ListRequest.class );
            if ( request.adapterIds() == null || request.adapterIds().stream().anyMatch( id -> id == null || id < 0 ) ) {
                throw MappingError.Validation.adapterIds();
            }
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                var snapshot = catalog.getSnapshot();
                return new MappingModel.ListResponse( request.adapterIds().stream().distinct().map( adapterId -> {
                    var adapter = snapshot.getAdapter( adapterId ).orElseThrow( MappingError.Missing::resource );
                    if ( adapter.type != AdapterType.SOURCE ) {
                        throw MappingError.Missing.resource();
                    }
                    var summaries = snapshot.mappings().getMappings().stream()
                            .filter( mapping -> mapping.kind() == MappingKind.PHYSICAL_TO_LOGICAL )
                            .filter( mapping -> SchemaMappingManager.sourceAllocations( catalog, mapping ).stream().anyMatch( a -> a.adapterId == adapterId ) )
                            .map( mapping -> summary( catalog, mapping.id() ) ).toList();
                    return new MappingModel.AdapterMappings( adapterId, adapter.uniqueName, summaries );
                } ).toList() );
            }
        } );
    }


    /**
     * Loads a saved mapping belonging to the specified adapter and returns its full details for the view or edit screen
     */
    public void getAdapterMapping( Context context ) {
        respond( context, () -> {
            DetailRequest request = read( context, DetailRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                return detail( catalog, request.mappingId(), request.adapterId() );
            }
        } );
    }


    /**
     * Update adapter mapping
     */
    public void updateRelMapping( Context context ) {
        respond( context, () -> {
            RelUpdateRequest request = read( context, RelUpdateRequest.class );
            return transact( transaction -> MappingModel.RelDetail.from( DdlManager.getInstance()
                    .updateRelMapping( request.toCommand(), transaction.createStatement() ), Catalog.getInstance() ) );
        } );
    }


    public void validateRelMappingConversions( Context context ) {
        respond( context, () -> {
            var request = read( context, RelValidationRequest.class );
            Catalog catalog = Catalog.getInstance();
            RelMappingState state;
            synchronized ( catalog ) {
                if ( request.mappingId() != null ) {
                    if ( request.mappingId() < 0 || request.targetEntityId() != null
                            || request.existingTargetEntityId() != null || request.sourceAllocationId() != null ) {
                        throw MappingError.Validation.request();
                    }
                    state = RelMappingState.load( catalog, request.adapterId(), request.mappingId() );
                } else {
                    if ( request.targetEntityId() == null ) {
                        throw MappingError.Validation.request();
                    }
                    state = RelMappingState.draft( catalog, request.adapterId(), request.targetEntityId(), request.existingTargetEntityId(), request.sourceAllocationId() );
                }
                if ( !state.editable() ) {
                    throw MappingError.Unsupported.editor( state.readOnlyReason() );
                }
                state.validateRevision( request.expectedRevision() );
            }
            Transaction transaction = crud.getTransaction();
            try {
                return RelMappingConversionValidator.validate( state, request.columns(), transaction.createStatement() );
            } finally {
                if ( transaction.isActive() ) {
                    transaction.rollback( "Mapping conversion validation finished" );
                }
            }
        } );
    }


    private <T> T transact( Function<Transaction, T> action ) {
        Transaction transaction = crud.getTransaction();
        try {
            return action.apply( transaction );
        } catch ( Exception e ) {
            if ( transaction.isActive() ) {
                try {
                    transaction.rollback( "Mapping edit failed" );
                } catch ( Exception rollbackError ) {
                    e.addSuppressed( rollbackError );
                }
            }
            throw e;
        }
    }


    private static MappingModel.Summary summary( Catalog catalog, long mappingId ) {
        var mapping = catalog.getSnapshot().mappings().getMapping( mappingId ).orElseThrow( () -> MappingError.Missing.mapping( mappingId ) );
        return switch ( mapping.targetModel() ) {
            case RELATIONAL -> MappingModel.Summary.from( RelMappingState.load( catalog, mappingId ) );
            case DOCUMENT -> MappingModel.Summary.from( DocumentMappingState.load( catalog, mappingId ) );
            case GRAPH -> throw MappingError.Unsupported.models( mapping.id(), mapping.sourceModel(), mapping.targetModel() );
        };
    }


    private static Object detail( Catalog catalog, long mappingId, Long adapterId ) {
        var mapping = catalog.getSnapshot().mappings().getMapping( mappingId ).orElseThrow( () -> MappingError.Missing.mapping( mappingId ) );
        return switch ( mapping.targetModel() ) {
            case RELATIONAL -> relDetail( catalog, mappingId, adapterId );
            case DOCUMENT -> documentDetail( catalog, mappingId, adapterId );
            case GRAPH -> throw MappingError.Unsupported.models( mapping.id(), mapping.sourceModel(), mapping.targetModel() );
        };
    }


    private static MappingModel.RelDetail relDetail( Catalog catalog, long mappingId, Long adapterId ) {
        var state = adapterId == null ? RelMappingState.load( catalog, mappingId ) : RelMappingState.load( catalog, adapterId, mappingId );
        return MappingModel.RelDetail.from( state, catalog );
    }


    private static MappingModel.DocumentDetail documentDetail( Catalog catalog, long mappingId, Long adapterId ) {
        var state = DocumentMappingState.load( catalog, mappingId );
        if ( adapterId != null && adapterId != state.adapter().id ) {
            throw MappingError.Missing.resource();
        }
        return MappingModel.DocumentDetail.from( state );
    }


    private static <T> T read( Context context, Class<T> type ) {
        return parseRequest( context.body(), type );
    }


    static <T> T parseRequest( String body, Class<T> type ) {
        try {
            T request = REQUEST_MAPPER.readValue( body, type );
            if ( request == null ) {
                throw MappingError.Validation.request();
            }
            return request;
        } catch ( JsonProcessingException e ) {
            throw MappingError.Validation.malformedRequest( e );
        }
    }


    static void respond( Context context, Supplier<Object> action ) {
        try {
            context.json( action.get() );
        } catch ( RuntimeException e ) {
            MappingException mappingError = MappingException.findCause( e ).orElseGet( () -> MappingError.Operation.requestFailed( e ) );
            int status;
            if ( mappingError instanceof MappingMissingException ) {
                status = 404;
            } else if ( mappingError instanceof MappingDuplicateException || mappingError instanceof MappingRevisionException
                    || mappingError instanceof MappingConflictException || mappingError instanceof MappingIncompatibleException ) {
                status = 409;
            } else if ( mappingError instanceof MappingValidationException || mappingError instanceof MappingUnsupportedException ) {
                status = 400;
            } else {
                status = 500;
            }
            if ( status == 500 ) {
                log.error( "Mapping request failed", e );
            }
            context.status( status ).json( new MappingModel.Error( mappingError.getCode(), mappingError.getMessage(), mappingError.fieldErrors, mappingError.blockingEntities ) );
        }
    }

}
