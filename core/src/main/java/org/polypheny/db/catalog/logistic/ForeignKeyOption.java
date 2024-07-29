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

public enum ForeignKeyOption {
    NONE( -1 ),
    // IDs according to JDBC standard
    //CASCADE( 0 ),
    RESTRICT( 1 );
    //SET_NULL( 2 ),
    //SET_DEFAULT( 4 );

    private final int id;


    ForeignKeyOption( int id ) {
        this.id = id;
    }


    public int getId() {
        return id;
    }


    public static ForeignKeyOption getById( int id ) {
        for ( ForeignKeyOption e : values() ) {
            if ( e.id == id ) {
                return e;
            }
        }
        throw new RuntimeException( "Unknown ForeignKeyOption with id: " + id );
    }


    public static ForeignKeyOption parse( @NonNull String str ) {
        if ( str.equalsIgnoreCase( "NONE" ) ) {
            return ForeignKeyOption.NONE;
        } else if ( str.equalsIgnoreCase( "RESTRICT" ) ) {
            return ForeignKeyOption.RESTRICT;
        } /*else if ( str.equalsIgnoreCase( "CASCADE" ) ) {
            return ForeignKeyOption.CASCADE;
        } else if ( str.equalsIgnoreCase( "SET NULL" ) ) {
            return ForeignKeyOption.SET_NULL;
        } else if ( str.equalsIgnoreCase( "SET DEFAULT" ) ) {
            return ForeignKeyOption.SET_DEFAULT;
        }*/
        throw new RuntimeException( "Unknown ForeignKeyOption with name: " + str );
    }
}
