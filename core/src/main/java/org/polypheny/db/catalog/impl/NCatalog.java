/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.impl;

import io.activej.serializer.annotations.SerializeClass;
import org.polypheny.db.catalog.impl.logical.DocumentCatalog;
import org.polypheny.db.catalog.impl.logical.GraphCatalog;
import org.polypheny.db.catalog.impl.logical.RelationalCatalog;
import org.polypheny.db.catalog.logistic.DataModel;

@SerializeClass(subclasses = { GraphCatalog.class, RelationalCatalog.class, DocumentCatalog.class }) // required for deserialization
public interface NCatalog {

    void commit();

    void rollback();

    boolean hasUncommittedChanges();

    DataModel getType();

    default <T extends NCatalog> T unwrap( Class<T> clazz ) {
        if ( !this.getClass().isAssignableFrom( clazz ) ) {
            throw new RuntimeException( String.format( "Error on retrieval the %s catalog.", clazz.getSimpleName() ) );
        }
        return clazz.cast( this );
    }

}