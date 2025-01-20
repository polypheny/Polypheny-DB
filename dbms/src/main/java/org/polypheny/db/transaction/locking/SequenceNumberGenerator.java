/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.locking;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;

public class SequenceNumberGenerator {

    private static final int WINDOW_SIZE = 1_000_000;

    private static volatile SequenceNumberGenerator instance;
    private final AtomicLong counter;

    private long lowestActive;
    private final BitSet releasedNumbers;


    private SequenceNumberGenerator() {
        this.counter = new AtomicLong( 0 );
        this.lowestActive = 1;
        this.releasedNumbers = new BitSet( WINDOW_SIZE );
    }


    public static SequenceNumberGenerator getInstance() {
        if ( instance == null ) {
            synchronized ( SequenceNumberGenerator.class ) {
                if ( instance == null ) {
                    instance = new SequenceNumberGenerator();
                }
            }
        }
        return instance;
    }


    public long getNextNumber() {
        return counter.incrementAndGet();
    }


    public synchronized void releaseNumber( long sequenceNumber ) {
        if ( sequenceNumber >= lowestActive && sequenceNumber < lowestActive + WINDOW_SIZE ) {
            releasedNumbers.set( (int) (sequenceNumber - lowestActive) );
        }
    }


    public synchronized long getLowestActive() {
        while ( true ) {
            int index = releasedNumbers.nextClearBit( 0 );
            if ( index >= WINDOW_SIZE ) {
                releasedNumbers.clear();
                lowestActive += WINDOW_SIZE;
            } else {
                return lowestActive + index;
            }
        }
    }

}
