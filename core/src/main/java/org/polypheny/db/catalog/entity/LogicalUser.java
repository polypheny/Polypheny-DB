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

package org.polypheny.db.catalog.entity;


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serial;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode
@Value
public class LogicalUser implements PolyObject, Comparable<LogicalUser> {

    @Serial
    private static final long serialVersionUID = 5022567585804699491L;

    @Serialize
    public long id;
    @Serialize
    public String name;
    @Serialize
    public String password;


    public LogicalUser( @Deserialize("id") final long id, @Deserialize("name") final String name, @Deserialize("password") final String password ) {
        this.id = id;
        this.name = name;
        this.password = password;
    }


    // Used for creating ResultSets
    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[]{ PolyString.of( name ) };
    }


    @Override
    public int compareTo( LogicalUser o ) {
        if ( o != null ) {
            return Math.toIntExact( this.id - o.id );
        }
        return -1;
    }

}
