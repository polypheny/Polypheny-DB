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

package org.polypheny.db.catalog.logistic;

import lombok.Getter;

@Getter
public enum EntityType {
    ENTITY( 1 ),
    SOURCE( 2 ),
    VIEW( 3 ),
    MATERIALIZED_VIEW( 4 );
    // STREAM, ...

    private final int id;


    EntityType( int id ) {
        this.id = id;
    }

}
