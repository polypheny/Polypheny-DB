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

package org.polypheny.db.adapter.neo4j;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.neo4j.types.NestedPolyType;


public enum NeoMethod {
    EXECUTE( NeoEntity.NeoQueryable.class, "execute", String.class, NestedPolyType.class, Map.class ),
    GRAPH_EXECUTE( NeoGraph.NeoQueryable.class, "execute", String.class, NestedPolyType.class, Map.class ),
    GRAPH_ALL( NeoGraph.NeoQueryable.class, "executeAll", String.class, String.class );

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
