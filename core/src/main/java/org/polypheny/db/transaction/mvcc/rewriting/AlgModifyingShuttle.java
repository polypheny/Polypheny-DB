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

package org.polypheny.db.transaction.mvcc.rewriting;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;

public class AlgModifyingShuttle extends AlgShuttleImpl {
    protected final Set<DeferredAlgTreeModification> pendingModifications;


    public AlgModifyingShuttle() {
        this.pendingModifications = new HashSet<>();
    }


    @Override
    protected <T extends AlgNode> T visitChild( T parent, int i, AlgNode child ) {
        T visited = super.visitChild( parent, i, child );
        return applyModificationsOrSkip( visited );
    }


    private <T extends AlgNode> T applyModificationsOrSkip( T node ) {
        if ( pendingModifications.isEmpty() ) {
            return node;
        }

        Iterator<DeferredAlgTreeModification> iterator = pendingModifications.iterator();
        while ( iterator.hasNext() ) {
            DeferredAlgTreeModification modification = iterator.next();
            if ( modification.notTargetsChildOf( node ) ) {
                continue;
            }
            node = (T) modification.applyToChild( node );
            iterator.remove();
        }
        return node;
    }

}
