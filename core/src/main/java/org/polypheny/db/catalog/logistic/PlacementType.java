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

import lombok.NonNull;

public enum PlacementType {
    MANUAL( 1 ),
    AUTOMATIC( 2 ),
    STATIC( 3 );

    private final int id;


    PlacementType( int id ) {
        this.id = id;
    }


    public int getId() {
        return id;
    }


    public static PlacementType getById( int id ) {
        for ( PlacementType e : values() ) {
            if ( e.id == id ) {
                return e;
            }
        }
        throw new RuntimeException( "Unknown PlacementType with id: " + id );
    }


    public static PlacementType parse( @NonNull String str ) {
        if ( str.equalsIgnoreCase( "MANUAL" ) ) {
            return PlacementType.MANUAL;
        } else if ( str.equalsIgnoreCase( "AUTOMATIC" ) ) {
            return PlacementType.AUTOMATIC;
        }
        throw new RuntimeException( "Unknown PlacementType with name: " + str );
    }


}
