/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.io;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * A helper class to reduce access to {@link AtomicBoolean#get()} to gain performance.
 */
public final class ParquetCancellation {

    private static final int CHECK_INTERVAL = 4096;


    private ParquetCancellation() {
    }


    public static boolean shouldStop( long row, AtomicBoolean cancelFlag ) {
        return (row & (CHECK_INTERVAL - 1)) == 0 && cancelFlag.get();
    }

}
