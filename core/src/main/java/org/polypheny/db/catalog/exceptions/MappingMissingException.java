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

import java.util.List;
import java.util.Map;

/**
 * indicates that a required mapping resource, such as a table, column, adapter, or allocation, does not exist.
 */
public final class MappingMissingException extends MappingException {

    MappingMissingException( String message, Throwable cause, Map<String, String> fieldErrors, List<String> blockingEntities ) {
        super( message, cause, fieldErrors, blockingEntities );
    }


    @Override
    public String getCode() {
        return "MISSING";
    }

}
