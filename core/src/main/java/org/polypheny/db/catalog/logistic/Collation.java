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
import lombok.NonNull;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;

@Getter
public enum Collation {
    CASE_SENSITIVE( 1 ),
    CASE_INSENSITIVE( 2 );

    public final int id;


    Collation( int id ) {
        this.id = id;
    }


    public static Collation getById( int id ) {
        for ( Collation c : values() ) {
            if ( c.id == id ) {
                return c;
            }
        }
        throw new GenericRuntimeException( "Unknown Collation with id: " + id );
    }


    public static Collation parse( @NonNull String str ) {
        if ( str.equalsIgnoreCase( "CASE SENSITIVE" ) ) {
            return Collation.CASE_SENSITIVE;
        } else if ( str.equalsIgnoreCase( "CASE INSENSITIVE" ) ) {
            return Collation.CASE_INSENSITIVE;
        }
        throw new GenericRuntimeException( "Unknown Collation with name: " + str );
    }


    public static Collation getDefaultCollation() {
        return getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() );
    }
}
