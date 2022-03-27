/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;

public enum NeoMethod {
    EXECUTE( NeoEntity.NeoQueryable.class, "execute", List.class, List.class, List.class );
    //MONGO_QUERYABLE_FIND( MongoTable.MongoQueryable.class, "find", String.class, String.class, List.class, List.class ),
    //MONGO_QUERYABLE_AGGREGATE( MongoTable.MongoQueryable.class, "aggregate", List.class, List.class, List.class, List.class, List.class ),
    //HANDLE_DIRECT_DML( MongoTable.MongoQueryable.class, "handleDirectDML", Operation.class, String.class, List.class, boolean.class, boolean.class );

    public final Method method;

    public static final ImmutableMap<Method, NeoMethod> MAP;


    static {
        final ImmutableMap.Builder<Method, NeoMethod> builder = ImmutableMap.builder();
        for ( NeoMethod value : NeoMethod.values() ) {
            builder.put( value.method, value );
        }
        MAP = builder.build();
    }


    NeoMethod( Class<?> clazz, String methodName, Class<?>... argumentTypes ) {
        this.method = Types.lookupMethod( clazz, methodName, argumentTypes );
    }
}
