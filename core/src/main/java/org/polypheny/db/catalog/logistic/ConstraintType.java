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

package org.polypheny.db.catalog.logistic;

import lombok.NonNull;

public enum ConstraintType {
    UNIQUE( 1 ),
    PRIMARY( 2 );

    private final int id;


    ConstraintType( int id ) {
        this.id = id;
    }


    public int getId() {
        return id;
    }


    public static ConstraintType getById( int id ) {
        for ( ConstraintType e : values() ) {
            if ( e.id == id ) {
                return e;
            }
        }
        throw new RuntimeException( "Unknown ConstraintType with id: " + id );
    }


    public static ConstraintType parse( @NonNull String str ) {
        if ( str.equalsIgnoreCase( "UNIQUE" ) ) {
            return ConstraintType.UNIQUE;
        }
        throw new RuntimeException( "Unknown ConstraintType with name: " + str );
    }
}
