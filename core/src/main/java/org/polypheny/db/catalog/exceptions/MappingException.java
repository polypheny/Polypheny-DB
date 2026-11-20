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

package org.polypheny.db.catalog.exceptions;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Base class for all Mapping Exceptions
 */
public abstract class MappingException extends GenericRuntimeException {

    // associates a form field with its error
    public final Map<String, String> fieldErrors;

    // contains the names of objects preventing an operation, such as dependent views or foreign keys.
    public final List<String> blockingEntities;


    protected MappingException( String message, Throwable cause, Map<String, String> fieldErrors, List<String> blockingEntities ) {
        // cause contains original exception
        super( message, cause );
        this.fieldErrors = Map.copyOf( fieldErrors );
        this.blockingEntities = List.copyOf( blockingEntities );
    }

    // code exposes the exception’s category to the frontend.
    public abstract String getCode();


    /**
     * Locate a MappingException when another layer has wrapped it.
     * Handles cases where another layer wraps MappingException and MappingException does not reach the handler directly
     */
    public static Optional<MappingException> findCause( Throwable error ) {
        Set<Throwable> visited = Collections.newSetFromMap( new IdentityHashMap<>() );
        for ( Throwable cause = error; cause != null && visited.add( cause ); cause = cause.getCause() ) {
            if ( cause instanceof MappingException mappingError ) {
                return Optional.of( mappingError );
            }
        }
        return Optional.empty();
    }

}
