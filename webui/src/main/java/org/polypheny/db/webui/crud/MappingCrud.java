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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.catalog.mapping.MappingState.EditException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.mapping.MappingModel;
import org.polypheny.db.webui.models.requests.MappingRequest.DetailRequest;
import org.polypheny.db.webui.models.requests.MappingRequest.ListRequest;
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
                            .filter( mapping -> snapshot.alloc().getFromLogical( mapping.targetEntityId() ).stream().anyMatch( a -> a.adapterId == adapterId ) )
                            .map( mapping -> MappingModel.Summary.from( MappingState.load( catalog, adapterId, mapping.id() ) ) ).toList();
                    return new MappingModel.AdapterMappings( adapterId, adapter.uniqueName, summaries );
                } ).toList() );
            }
        } );
    }


    public void getAdapterMapping( Context context ) {
        respond( context, () -> {
            DetailRequest request = read( context, DetailRequest.class );
            Catalog catalog = Catalog.getInstance();
            synchronized ( catalog ) {
                return MappingModel.Detail.from( MappingState.load( catalog, request.adapterId(), request.mappingId() ) );
            }
        } );
    }


    public void updateAdapterMapping( Context context ) {
        respond( context, () -> {
            UpdateRequest request = read( context, UpdateRequest.class );
            Transaction transaction = crud.getTransaction();
            try {
                var saved = DdlManager.getInstance().updateMapping( request.toCommand(), transaction.createStatement() );
                return MappingModel.Detail.from( saved );
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
        } );
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
