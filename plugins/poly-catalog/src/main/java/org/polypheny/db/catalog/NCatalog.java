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

package org.polypheny.db.catalog;

import io.activej.serializer.annotations.SerializeClass;
import org.polypheny.db.catalog.logical.document.DocumentCatalog;
import org.polypheny.db.catalog.logical.graph.GraphCatalog;
import org.polypheny.db.catalog.logical.relational.RelationalCatalog;

@SerializeClass(subclasses = { GraphCatalog.class, RelationalCatalog.class, DocumentCatalog.class }) // required for deserialization
public interface NCatalog {

    void commit();

    void rollback();

    boolean hasUncommittedChanges();

    Catalog.NamespaceType getType();

    default RelationalCatalog asRelational() {
        return unwrap( RelationalCatalog.class );
    }

    default DocumentCatalog asDocument() {
        return unwrap( DocumentCatalog.class );
    }

    default GraphCatalog asGraph() {
        return unwrap( GraphCatalog.class );
    }

    default <T extends NCatalog> T unwrap( Class<T> clazz ) {
        if ( !this.getClass().isAssignableFrom( clazz ) ) {
            throw new RuntimeException( String.format( "Error on retrieval the %s catalog.", clazz.getSimpleName() ) );
        }
        return clazz.cast( this );
    }


}
