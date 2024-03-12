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

package org.polypheny.db.adapter.cottontail.enumberable;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.cottontail.CottontailWrapper;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyValue;
import org.vitrivr.cottontail.grpc.CottontailGrpc.UpdateMessage;


@Slf4j
public class CottontailUpdateEnumerable extends AbstractEnumerable<PolyValue[]> {

    private final List<UpdateMessage> updates;
    private final CottontailWrapper wrapper;


    public CottontailUpdateEnumerable( List<UpdateMessage> updates, CottontailWrapper wrapper ) {
        this.updates = updates;
        this.wrapper = wrapper;
    }


    @Override
    public Enumerator<PolyValue[]> enumerator() {
        return new CottontailUpdateEnumerator();
    }


    private class CottontailUpdateEnumerator implements Enumerator<PolyValue[]> {

        /**
         * Result of the last UPDATE that was performed.
         */
        private PolyValue[] currentResult;

        /**
         * The pointer to the last {@link UpdateMessage} that was executed.
         */
        private int pointer = 0;


        @Override
        public PolyValue[] current() {
            return this.currentResult;
        }


        @Override
        public boolean moveNext() {
            if ( this.pointer < CottontailUpdateEnumerable.this.updates.size() ) {
                final UpdateMessage updateMessage = CottontailUpdateEnumerable.this.updates.get( this.pointer++ );
                this.currentResult = new PolyValue[]{ PolyLong.of( CottontailUpdateEnumerable.this.wrapper.update( updateMessage ) ) };
                return !currentResult[0].equals( PolyLong.of( -1 ) );
            }
            return false;
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
