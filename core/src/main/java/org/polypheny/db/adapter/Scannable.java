/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;

public interface Scannable {

    AlgNode getRelScan( long allocId, AlgBuilder builder );

    AlgNode getGraphScan( long allocId, AlgBuilder builder );

    AlgNode getDocumentScan( long allocId, AlgBuilder builder );

    AlgNode getScan( long allocId, AlgBuilder builder );

    default void createTable( Context context, LogicalTableWrapper logical, List<AllocationTableWrapper> allocations ) {
        for ( AllocationTableWrapper allocation : allocations ) {
            createTable( context, logical, allocation );
        }
    }

    void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

    void refreshTable( long allocId );

    void refreshGraph( long allocId );

    void refreshCollection( long allocId );

}
