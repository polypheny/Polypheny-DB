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

import lombok.NonNull;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.DeadlockException;

public interface Lockable {

    enum LockState {
        SHARED,
        EXCLUSIVE,
        MVCC
    }


    enum LockType {
        SHARED,
        EXCLUSIVE,
        MVCC
    }

    void acquire( @NonNull Transaction transaction, @NonNull LockType lockType ) throws DeadlockException;

    void release( @NonNull Transaction transaction );

    LockType getLockType();

    boolean isRoot();

    boolean isLocked();

}
