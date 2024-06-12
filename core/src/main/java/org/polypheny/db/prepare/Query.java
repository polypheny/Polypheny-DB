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

package org.polypheny.db.prepare;

import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.algebra.AlgNode;

/**
 * A union type of the three possible ways of expressing a query: as a SQL string, a {@link Queryable} or a {@link AlgNode}. Exactly one must be provided.
 *
 * @param <T> element type
 */
public class Query<T> {

    public final String sql;
    public final Queryable<T> queryable;
    public final AlgNode alg;


    private Query( String sql, Queryable<T> queryable, AlgNode alg ) {
        this.sql = sql;
        this.queryable = queryable;
        this.alg = alg;

        assert (sql == null ? 0 : 1) + (queryable == null ? 0 : 1) + (this.alg == null ? 0 : 1) == 1;
    }


    public static <T> Query<T> of( String sql ) {
        return new Query<>( sql, null, null );
    }


    public static <T> Query<T> of( Queryable<T> queryable ) {
        return new Query<>( null, queryable, null );
    }


    public static <T> Query<T> of( AlgNode alg ) {
        return new Query<>( null, null, alg );
    }

}
