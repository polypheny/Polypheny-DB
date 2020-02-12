/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.CqlSession;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;


/**
 * Builtin methods in the Cassandra adapter.
 */
public enum CassandraMethod {

    CASSANDRA_QUERYABLE_QUERY( CassandraTable.CassandraQueryable.class, "query", List.class, List.class, List.class, List.class, Integer.class, Integer.class ),
    CASSANDRA_STRING_ENUMERABLE( CassandraEnumerable.class, "of", CqlSession.class, String.class ),
    CASSANDRA_STRING_ENUMERABLE_OFFSET( CassandraEnumerable.class, "of", CqlSession.class, String.class, Integer.class );
//    CASSANDRA_STRING_ENUMERABLE(CassandraTable.CassandraQueryable.class, "insert", String.class );

    public final Method method;

    public static final ImmutableMap<Method, CassandraMethod> MAP;


    static {
        final ImmutableMap.Builder<Method, CassandraMethod> builder = ImmutableMap.builder();
        for ( CassandraMethod value : CassandraMethod.values() ) {
            builder.put( value.method, value );
        }
        MAP = builder.build();
    }


    CassandraMethod( Class clazz, String methodName, Class... argumentTypes ) {
        this.method = Types.lookupMethod( clazz, methodName, argumentTypes );
    }
}

