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

import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;


/**
 * Neo4j representation of a {@link Enumerator}.
 *
 * @param <T>
 */
public class NeoEnumerator<T> implements Enumerator<T> {

    private final List<Result> results;
    private final Function1<Record, T> getter;
    private Result result;
    private T current;
    private int pos = 0;


    public NeoEnumerator( List<Result> results, Function1<Record, T> getter ) {
        this.results = results;
        this.result = results.get( pos );
        pos++;
        this.getter = getter;
    }


    @Override
    public T current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        if ( result.hasNext() ) {
            this.current = getter.apply( result.next() );
            return true;
        }
        if ( results.size() > pos ) {
            this.result = results.get( pos );
            pos++;

            return moveNext();
        }

        return false;
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        this.result.consume();
    }

}
