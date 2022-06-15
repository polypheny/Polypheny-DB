/*
 * Copyright 2019-2022 The Polypheny Project
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

import lombok.Getter;

public class ProcedureAlreadyExistsException extends CatalogException {

    @Getter
    private final String procedureName;


    public ProcedureAlreadyExistsException(String databaseName, String schemaName, String procedureName ) {
        super( "Procedure with name '" + procedureName + "' already exists in schema '" + schemaName + "' of database '" + databaseName + "'." );
        this.procedureName = procedureName;
    }


    public ProcedureAlreadyExistsException(long databaseId, long schemaId, String procedureName ) {
        super( "Procedure with name '" + procedureName + "' already exists in schema '" + schemaId + "' of database '" + databaseId + "'");
        this.procedureName = procedureName;
    }


    public ProcedureAlreadyExistsException(long schemaId, String procedureName ) {
        super( "Procedure with name '" + procedureName + "' already exists in schema '" + schemaId );
        this.procedureName = procedureName;
    }

}
