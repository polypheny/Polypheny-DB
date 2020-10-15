/*
 * Copyright 2019-2020 The Polypheny Project
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


public class UnknownTableException extends CatalogException {

    public UnknownTableException( String databaseName, String schemaName, String tableName ) {
        super( "There is no table with name '" + tableName + "' in schema '" + schemaName + "' of database '" + databaseName + "'." );
    }


    public UnknownTableException( long databaseId, String schemaName, String tableName ) {
        super( "There is no table with name '" + tableName + "' in schema '" + schemaName + "' of database with the id '" + databaseId + "'." );
    }


    public UnknownTableException( long schemaId, String tableName ) {
        super( "There is no table with name '" + tableName + "' in the schema with the id '" + schemaId + "'." );
    }


    public static class UnknownTableIdRuntimeException extends CatalogRuntimeException {

        public UnknownTableIdRuntimeException( long tableId ) {
            super( "There is no table with id '" + tableId + "'." );
        }

    }

}
