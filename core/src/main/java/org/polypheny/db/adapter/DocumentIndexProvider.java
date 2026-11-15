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

package org.polypheny.db.adapter;

import java.util.List;

/**
 * Optional metadata capability of a physical document entity. Only indexes actually
 * present in the adapter are reported; relational storage indexes are not document indexes.
 */
public interface DocumentIndexProvider {

    List<DocumentIndexMetadata> getDocumentIndexes();


    /**
     * One indexed field, in document path notation. Uniqueness is reported only for
     * single-field indexes: a compound index does not make its individual fields
     * unique. For sparse or filtered indexes it applies only to indexed documents.
     */
    record DocumentIndexMetadata( String fieldPath, String indexType, boolean unique ) {

    }

}
