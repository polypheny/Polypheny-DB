/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail.enumberable;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.vitrivr.cottontail.client.iterators.Tuple;
import org.vitrivr.cottontail.client.iterators.TupleIterator;

import java.util.LinkedList;
import java.util.List;


public class CottontailQueryEnumerable<T> extends AbstractEnumerable<T> {

    private final TupleIterator tupleIterator;

    public CottontailQueryEnumerable(TupleIterator iterator ) {
        this.tupleIterator = iterator;
    }

    @Override
    public Enumerator<T> enumerator() {
        return new CottontailQueryResultEnumerator( this.tupleIterator );
    }


    private static class CottontailQueryResultEnumerator<T> implements Enumerator<T> {

        /** The current {@link TupleIterator} backing this {@link CottontailQueryEnumerable}. */
        private final TupleIterator tupleIterator;

        /** The current names of the columns returned by this {@link CottontailQueryResultEnumerator}. */
        private final List<String> columns;

        /** The current {@link Tuple} this {@link CottontailQueryEnumerable} is pointing to. */
        private Tuple tuple = null;

        CottontailQueryResultEnumerator( TupleIterator tupleIterator ) {
            this.tupleIterator = tupleIterator;
            this.columns = new LinkedList<>(tupleIterator.getColumns());
        }


        @Override
        public T current() {
            final Object[] returnValue = new Object[this.columns.size()];
            for ( int i = 0; i < returnValue.length; i++ ) {
                String columnName = this.columns.get( i );
                returnValue[i] = this.tuple.get( columnName );
            }
            return ( (T) returnValue );
        }


        @Override
        public boolean moveNext() {
            if (this.tupleIterator.hasNext()) {
                this.tuple = this.tupleIterator.next();
                return true;
            } else {
                return false;
            }
        }


        @Override
        public void reset() {
            // TODO js(ct): do we need to do something here?
        }


        @Override
        public void close() {
            try {
                this.tupleIterator.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
