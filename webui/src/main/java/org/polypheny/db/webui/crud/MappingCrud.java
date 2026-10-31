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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.mapping.MappingState.EditException;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.mapping.MappingModel;
import org.polypheny.db.webui.models.requests.MappingRequest.CreateRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.DetailRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.ListRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.MappingIdRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.OverviewRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.RemoveRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.SourceRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.UpdateRequest;

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
                return new MappingModel.OverviewResponse( snapshot.mappings().getMappings().stream()
                        .filter( mapping -> request.targetEntityId() == null || mapping.targetEntityId() == request.targetEntityId() )
                        .filter( mapping -> request.adapterId() == null || SchemaMappingManager.sourceAllocations( catalog, mapping ).stream()
                                .anyMatch( allocation -> allocation.adapterId == request.adapterId() ) )
                        .map( mapping -> MappingModel.Summary.from( MappingState.load( catalog, mapping.id() ) ) )
                        .sorted( Comparator.comparing( MappingModel.Summary::mappingName ).thenComparingLong( MappingModel.Summary::mappingId ) ).toList() );
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
                return MappingModel.Detail.from( MappingState.load( catalog, request.mappingId() ) );
            }
        } );
    }

    /**
     * Returns the available sources for creating a mapping, optionally filtering by adapter and excluding sources already mapped to a specified target.
     */
    public void getMappingSources( Context context ) {
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
                        .distinct().map( source -> MappingState.draft( catalog, source.adapterId, source.logicalId, null, source.id ) )
                        .filter( MappingState::editable )
                        .filter( state -> request.targetEntityId() == null || !SchemaMappingManager.isPhysicalSourceMappedToTarget( catalog, state.mapping(), request.targetEntityId() ) )
                        .filter( state -> seenSources.add( SchemaMappingManager.physicalSources( catalog, state.mapping() ) ) )
                        .map( MappingModel.Source::from ).toList();
                return new MappingModel.SourcesResponse( sources );
            }
        } );
    }


    /**
     * Prepares a proposed mapping for the Create Mapping screen
     */
    public void getMappingDraft( Context context ) {
        respond( context, () -> {
            var request = read( context, SourceRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                var state = MappingState.draft( catalog, request.adapterId(), request.targetEntityId(), request.existingTargetEntityId(), request.sourceAllocationId() );
                if ( !state.editable() ) {
                    throw new EditException( "UNSUPPORTED_MAPPING", state.readOnlyReason() );
                }
                return MappingModel.Detail.from( state );
            }
        } );
    }


    /**
     * Validates the submitted mapping draft and creates a saved mapping,
     * either creating a new target table or connecting the selected source to an existing target
     */
    public void createMapping( Context context ) {
        respond( context, () -> {
            var request = read( context, CreateRequest.class );
            return transact( transaction -> MappingModel.Detail.from( DdlManager.getInstance()
                    .createMapping( request.targetEntityId(), request.toCommand(), transaction.createStatement() ) ) );
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
            throw MappingState.notFound();
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
                throw new EditException( "VALIDATION_FAILED", "Supply valid source adapter IDs." );
            }
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                var snapshot = catalog.getSnapshot();
                return new MappingModel.ListResponse( request.adapterIds().stream().distinct().map( adapterId -> {
                    var adapter = snapshot.getAdapter( adapterId ).orElseThrow( MappingState::notFound );
                    if ( adapter.type != AdapterType.SOURCE ) {
                        throw MappingState.notFound();
                    }
                    var summaries = snapshot.mappings().getMappings().stream()
                            .filter( mapping -> mapping.kind() == MappingKind.PHYSICAL_TO_LOGICAL )
                            .filter( mapping -> SchemaMappingManager.sourceAllocations( catalog, mapping ).stream().anyMatch( a -> a.adapterId == adapterId ) )
                            .map( mapping -> MappingModel.Summary.from( MappingState.load( catalog, adapterId, mapping.id() ) ) ).toList();
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
                return MappingModel.Detail.from( MappingState.load( catalog, request.adapterId(), request.mappingId() ) );
            }
        } );
    }


    /**
     * Update adapter mapping
     */
    public void updateMapping( Context context ) {
        respond( context, () -> {
            UpdateRequest request = read( context, UpdateRequest.class );
            return transact( transaction -> MappingModel.Detail.from( DdlManager.getInstance()
                    .updateMapping( request.toCommand(), transaction.createStatement() ) ) );
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


    private static <T> T read( Context context, Class<T> type ) {
        return parseRequest( context.body(), type );
    }


    static <T> T parseRequest( String body, Class<T> type ) {
        try {
            T request = REQUEST_MAPPER.readValue( body, type );
            if ( request == null ) {
                throw new EditException( "VALIDATION_FAILED", "A JSON request object is required." );
            }
            return request;
        } catch ( JsonProcessingException e ) {
            throw new EditException( "VALIDATION_FAILED", "The mapping request is malformed, incomplete, or contains unsupported fields." );
        }
    }


    private static void respond( Context context, Supplier<Object> action ) {
        try {
            context.json( action.get() );
        } catch ( EditException e ) {
            int status = switch ( e.code ) {
                case "NOT_FOUND" -> 404;
                case "REVISION_CONFLICT", "DEPENDENCY_CONFLICT", "SOURCE_UNAVAILABLE" -> 409;
                default -> 400;
            };
            context.status( status ).json( new MappingModel.Error( e.code, e.getMessage(), e.fieldErrors, e.blockingEntities ) );
        } catch ( RuntimeException e ) {
            log.error( "Mapping request failed", e );
            context.status( 500 ).json( new MappingModel.Error( "UPDATE_FAILED",
                    "The mapping request could not be completed. Reload the saved version to check its state.", Map.of(), List.of() ) );
        }
    }

}
