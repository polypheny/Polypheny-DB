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

package org.polypheny.db.adapter.parquet.relational.execution;


/**
 * Shared runtime state for a limit that applies to parent rows before nested
 * join expansion.
 */
public final class ParentLimitState {

    private long rowsToSkip;
    private long rowsRemaining;


    public ParentLimitState( int offset, int fetch ) {
        this.rowsToSkip = Math.max( 0, offset );
        this.rowsRemaining = fetch < 0 ? Long.MAX_VALUE : fetch;
    }


    @SuppressWarnings("unused")
    public static ParentLimitState unlimited() {
        return new ParentLimitState( 0, -1 );
    }


    public synchronized boolean includeNextParentRow() {
        if ( rowsToSkip > 0 ) {
            rowsToSkip--;
            return false;
        }
        if ( rowsRemaining <= 0 ) {
            return false;
        }
        if ( rowsRemaining != Long.MAX_VALUE ) {
            rowsRemaining--;
        }
        return true;
    }

}
