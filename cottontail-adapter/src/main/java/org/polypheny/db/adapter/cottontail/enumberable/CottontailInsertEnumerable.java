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
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;


public class CottontailInsertEnumerable extends AbstractEnumerable<Long> {

    private final List<InsertMessage> inserts;
    private final CottontailWrapper wrapper;


    public CottontailInsertEnumerable( List<InsertMessage> inserts, CottontailWrapper wrapper ) {
        this.inserts = inserts;
        this.wrapper = wrapper;
    }


    @Override
    public Enumerator<Long> enumerator() {
        return new CottontailInsertResultEnumerator();
    }


    private class CottontailInsertResultEnumerator implements Enumerator<Long> {

        /**
         * Result of the last INSERT that was performed.
         */
        private long currentResult;

        /**
         * The pointer to the last {@link InsertMessage} that was executed.
         */
        private int pointer = 0;


        @Override
        public Long current() {
            return this.currentResult;
        }


        @Override
        public boolean moveNext() {
            if ( this.pointer < CottontailInsertEnumerable.this.inserts.size() ) {
                final InsertMessage insertMessage = CottontailInsertEnumerable.this.inserts.get( this.pointer++ );
                if ( CottontailInsertEnumerable.this.wrapper.insert( insertMessage ) ) {
                    this.currentResult = 1;
                } else {
                    this.currentResult = -1;
                }
                return !(this.currentResult == -1L);
            } else {
                return false;
            }
        }


        @Override
        public void reset() {
            this.pointer = 0;
        }


        @Override
        public void close() {
        }

    }

}
