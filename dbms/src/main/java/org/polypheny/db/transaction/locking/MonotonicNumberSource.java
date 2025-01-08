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

import java.util.concurrent.atomic.AtomicLong;

public class MonotonicNumberSource {

    private static volatile MonotonicNumberSource instance;
    private final AtomicLong counter;


    private MonotonicNumberSource() {
        counter = new AtomicLong( 0 );
    }


    public static MonotonicNumberSource getInstance() {
        if ( instance == null ) {
            synchronized ( MonotonicNumberSource.class ) {
                if ( instance == null ) {
                    instance = new MonotonicNumberSource();
                }
            }
        }
        return instance;
    }


    public long getNextNumber() {
        return counter.incrementAndGet();
    }
}
