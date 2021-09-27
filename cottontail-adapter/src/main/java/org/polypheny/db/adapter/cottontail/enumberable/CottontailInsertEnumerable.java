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
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;

import java.util.List;


public class CottontailInsertEnumerable<T> extends AbstractEnumerable<T> {
    private final List<InsertMessage> inserts;
    private final CottontailWrapper wrapper;

    public CottontailInsertEnumerable( List<InsertMessage> inserts, CottontailWrapper wrapper ) {
        this.inserts = inserts;
        this.wrapper = wrapper;
    }

    @Override
    public Enumerator<T> enumerator() {
        return new CottontailInsertResultEnumerator<>();
    }

    private class CottontailInsertResultEnumerator<T> implements Enumerator<T> {
        private boolean wasSuccessful;
        private int checkCount = 0;

        @SuppressWarnings("unchecked")
        @Override
        public T current() {
            if ( this.wasSuccessful ) {
                return (T) Integer.valueOf( CottontailInsertEnumerable.this.inserts.size() );
            } else {
                return (T) Integer.valueOf( -1 );
            }
        }

        @Override
        public boolean moveNext() {
            if ( this.checkCount < CottontailInsertEnumerable.this.inserts.size() ) {
                this.wasSuccessful = CottontailInsertEnumerable.this.wrapper.insert( CottontailInsertEnumerable.this.inserts.get(this.checkCount++) );
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void reset() {
            this.checkCount = 0;
        }

        @Override
        public void close() {}
    }
}