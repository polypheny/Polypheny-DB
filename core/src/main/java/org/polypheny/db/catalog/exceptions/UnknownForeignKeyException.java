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


public class UnknownForeignKeyException extends CatalogException {

    public UnknownForeignKeyException( String name ) {
        super( "There is no foreign key with name: " + name );
    }


    public UnknownForeignKeyException( long id ) {
        super( "There is no foreign key with id: " + id );
    }

    public UnknownForeignKeyException( long tableId, String name ) {
        super( "There is no foreign key on table: " + tableId + " with name: " + name );
    }
}
