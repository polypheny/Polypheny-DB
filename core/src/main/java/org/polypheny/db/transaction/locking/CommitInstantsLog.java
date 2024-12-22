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

    private static HashMap<VersionedEntryIdentifier, Long> lastCommit = new HashMap<>();


    private VersionedEntryIdentifier getIdentifier( long entryIdentifier, long version ) {
        return new VersionedEntryIdentifier( entryIdentifier, version );
    }


    public void setOrUpdateLastCommit( long entryIdentifier, long version, long instant ) {
        VersionedEntryIdentifier identifier = getIdentifier( entryIdentifier, version );
        lastCommit.put( identifier, instant );
    }


    public long getLastCommit( long entryIdentifier, long version ) {
        VersionedEntryIdentifier identifier = getIdentifier( entryIdentifier, version );
        return lastCommit.get( identifier );
    }


    public void removeEntry( long entryIdentifier, long version ) {
        VersionedEntryIdentifier identifier = getIdentifier( entryIdentifier, version );
        lastCommit.remove( identifier );
    }

}
