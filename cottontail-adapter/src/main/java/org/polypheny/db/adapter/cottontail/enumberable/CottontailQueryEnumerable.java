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

import java.util.ArrayList;
import java.util.List;


public class CottontailQueryEnumerable<T> extends AbstractEnumerable<T> {

    private final TupleIterator tupleIterator;

    public CottontailQueryEnumerable(TupleIterator iterator ) {
        this.tupleIterator = iterator;
    }

    @Override
    public Enumerator<T> enumerator() {
        return new CottontailQueryResultEnumerator<>( this.tupleIterator );
    }


    private static class CottontailQueryResultEnumerator<T> implements Enumerator<T> {

        /** The current {@link TupleIterator} backing this {@link CottontailQueryEnumerable}. */
        private final TupleIterator tupleIterator;

        /** The current {@link Tuple} this {@link CottontailQueryEnumerable} is pointing to. */
        private Tuple tuple = null;

        CottontailQueryResultEnumerator( TupleIterator tupleIterator ) {
            this.tupleIterator = tupleIterator;
        }


        @Override
        public T current() {
            final Object[] returnValue = new Object[this.tupleIterator.getNumberOfColumns()];
            for ( int i = 0; i < this.tupleIterator.getNumberOfColumns(); i++ ) {
                returnValue[i] = convert( this.tuple.get( i ) ) ;

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

        /**
         * Internal conversion method that mainly converts arrays to lists.
         *
         * @param object The object to convert.
         * @return Converted object.
         */
        private Object convert(Object object) {
            if (object instanceof double[]) {
                final List<Double> list = new ArrayList<>(((double[]) object).length);
                for (double v : ((double[]) object)) {
                    list.add(v);
                }
                return list;
            } else if (object instanceof float[]) {
                final List<Float> list = new ArrayList<>(((float[]) object).length);
                for (float v : ((float[]) object)) {
                    list.add(v);
                }
                return list;
            } else if (object instanceof long[]) {
                final List<Long> list = new ArrayList<>(((long[]) object).length);
                for (long v : ((long[]) object)) {
                    list.add(v);
                }
                return list;
            } else if (object instanceof int[]) {
                final List<Integer> list = new ArrayList<>(((int[]) object).length);
                for (int v : ((int[]) object)) {
                    list.add(v);
                }
                return list;
            } else if (object instanceof boolean[]) {
                final List<Boolean> list = new ArrayList<>(((boolean[]) object).length);
                for (boolean v : ((boolean[]) object)) {
                    list.add(v);
                }
                return list;
            } else  {
                return object;
            }
        }
    }
}
