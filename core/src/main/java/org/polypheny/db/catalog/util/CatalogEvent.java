/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.catalog.util;

public enum CatalogEvent {
    ///////////////////////////////////////////
    // relational /////////////////////////////
    ///////////////////////////////////////////

    //- logical -----------------------------//
    LOGICAL_REL_ENTITY_CREATED(),
    LOGICAL_REL_ENTITY_DROPPED(),
    LOGICAL_REL_ENTITY_RENAMED(),
    LOGICAL_REL_FIELD_CREATED(),
    LOGICAL_REL_FIELD_DROPPED(),
    LOGICAL_REL_FIELD_RENAMED(),

    LOGICAL_REL_FIELD_TYPE_CHANGED(),

    LOGICAL_REL_FIELD_NULLABILITY_CHANGED(),

    LOGICAL_REL_FIELD_DEFAULT_VALUE_CHANGED(),

    LOGICAL_REL_FIELD_DEFAULT_VALUE_DROPPED(),

    LOGICAL_REL_FIELD_POSITION_CHANGED(),

    LOGICAL_REL_FIELD_COLLATION_CHANGED(),

    ///////////////////////////////////////////
    // document ///////////////////////////////
    ///////////////////////////////////////////
    LOGICAL_DOC_ENTITY_CREATED(),
    LOGICAL_DOC_ENTITY_DROPPED(),
    LOGICAL_DOC_ENTITY_RENAMED(),

    //////////////////////////////////////////
    // graph /////////////////////////////////
    //////////////////////////////////////////

    LOGICAL_GRAPH_ENTITY_CREATED(),
    LOGICAL_GRAPH_ENTITY_DROPPED(),
    LOGICAL_GRAPH_ENTITY_RENAMED(),

    // view

    VIEW_CREATED(),
    VIEW_DROPPED(),
    VIEW_RENAMED(),

    // materialized view

    MATERIALIZED_VIEW_CREATED(),
    MATERIALIZED_VIEW_DROPPED(),
    MATERIALIZED_VIEW_RENAMED(),

    MATERIALIZED_VIEW_UPDATED(),

    // namespace
    NAMESPACE_CREATED(),
    NAMESPACE_DROPPED(),
    NAMESPACE_RENAMED(),

    // keys

    KEY_CREATED(),
    KEY_DROPPED(),

    // foreign key

    FOREIGN_KEY_CREATED(),
    FOREIGN_KEY_DROPPED(),

    // primary key

    PRIMARY_KEY_CREATED(),
    PRIMARY_KEY_DROPPED(),

    // user
    USER_CREATED(),
    USER_DROPPED(),

    // index

    INDEX_CREATED(),
    INDEX_DROPPED(),

    INDEX_RENAMED(),

    // constraint

    CONSTRAINT_CREATED(),
    CONSTRAINT_DROPPED(),


    CatalogEvent() {
    }
}
