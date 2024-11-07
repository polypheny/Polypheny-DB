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

import java.util.Arrays;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

@Getter
public enum DataPlacementRole {
    UP_TO_DATE( 0 ),
    REFRESHABLE( 1 );

    private final int id;


    DataPlacementRole( int id ) {
        this.id = id;
    }


    public static DataPlacementRole from( final int id ) {
        return Arrays.stream( values() ).filter( t -> t.id == id ).findAny().orElseThrow( () -> new GenericRuntimeException( "Unknown DataPlacementRole with id: " + id ) );
    }


    public static DataPlacementRole from( final String name ) {
        return Arrays.stream( values() ).filter( t -> t.name().equalsIgnoreCase( name ) ).findAny().orElseThrow( () -> new GenericRuntimeException( "Unknown PartitionType with name: " + name ) );
    }

}
