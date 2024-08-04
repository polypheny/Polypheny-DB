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

package org.polypheny.db.prisminterface.streaming;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class StreamIndex {

    private final HashMap<Long, PIOutputStream> index = new HashMap<>();
    private final AtomicLong streamIdGenerator = new AtomicLong();


    public long register( PIOutputStream streamableWrapper ) {
        long streamId = streamIdGenerator.getAndIncrement();
        index.put( streamId, streamableWrapper );
        return streamId;
    }


    public PIOutputStream get( long streamId ) {
        return index.get( streamId );
    }


    public void reset() {
        index.clear();
    }

}
