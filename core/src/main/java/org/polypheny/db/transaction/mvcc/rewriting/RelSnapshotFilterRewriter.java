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

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.catalog.entity.Entity;

public class RelSnapshotFilterRewriter extends SnapshotFilterRewriter {

    public RelSnapshotFilterRewriter( CommitState commitState, Entity entity ) {
        super(commitState, entity);
    }

    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        LogicalRelFilter filter1 = visitChild( filter, 0, filter.getInput() );
        if ( !filter1.isMvccLhsScopeFilter() ) {
            return filter1;
        }
        if (matchesTarget( filter1.getInput().getEntity() ) ) {
            pendingModifications.add( new RelCommitStateFilterMod( commitState, filter1) );
        }
        return filter1;
    }

}
