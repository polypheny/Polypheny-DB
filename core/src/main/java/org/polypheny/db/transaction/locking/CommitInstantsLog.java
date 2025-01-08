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

package org.polypheny.db.transaction.locking;

import java.util.HashMap;

public class CommitInstantsLog {

    public static final long NO_COMMIT_INSTANT = 0;
    private static final HashMap<EntryIdentifier, Long> lastCommit = new HashMap<>();

    public void setOrUpdateLastCommit( EntryIdentifier identifier, long instant ) {
        lastCommit.put( identifier, instant );
    }


    public long getLastCommit( EntryIdentifier identifier ) {
        return lastCommit.getOrDefault(identifier, NO_COMMIT_INSTANT);
    }


    public void removeEntry( EntryIdentifier identifier ) {
        lastCommit.remove( identifier );
    }

}
