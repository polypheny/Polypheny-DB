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

import java.util.List;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.vitrivr.cottontail.grpc.CottontailGrpc.BatchInsertMessage;

public class CottontailBatchInsertEnumerable<T> extends AbstractEnumerable<T> {
    private final List<BatchInsertMessage> inserts;
    private final CottontailWrapper wrapper;

    public CottontailBatchInsertEnumerable( List<BatchInsertMessage> inserts, CottontailWrapper wrapper ) {
        this.inserts = inserts;
        this.wrapper = wrapper;
    }

    @Override
    public Enumerator<T> enumerator() {
        return new CottontailInsertResultEnumerator<>();
    }

    private class CottontailInsertResultEnumerator<T> implements Enumerator<T> {
        private boolean wasSuccessful;
        private boolean executed;
        private int checkCount = 0;

        @SuppressWarnings("unchecked")
        @Override
        public T current() {
            if ( this.wasSuccessful ) {
                return (T) Integer.valueOf( CottontailBatchInsertEnumerable.this.inserts.size() );
            } else {
                return (T) Integer.valueOf( -1 );
            }
        }

        @Override
        public boolean moveNext() {
            if ( !this.executed ) {
                this.wasSuccessful = CottontailBatchInsertEnumerable.this.wrapper.insert( CottontailBatchInsertEnumerable.this.inserts.get(this.checkCount++) );
                this.executed = true;
                return this.wasSuccessful;
            } else {
                if ( this.checkCount < CottontailBatchInsertEnumerable.this.inserts.size() ) {
                    this.checkCount += 1;
                    return true;
                } else {
                    return false;
                }
            }
        }

        @Override
        public void reset() {}

        @Override
        public void close() {}
    }
}