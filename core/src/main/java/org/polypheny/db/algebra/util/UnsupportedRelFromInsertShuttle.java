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

package org.polypheny.db.algebra.util;

import java.util.Objects;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.plan.volcano.AlgSubset;

public class UnsupportedRelFromInsertShuttle extends AlgShuttleImpl {

    private final Long tableId;
    private boolean containsOtherTableId = false;


    private UnsupportedRelFromInsertShuttle( Long tableId ) {
        this.tableId = tableId;
    }


    public static boolean contains( RelModify<?> modify ) {
        UnsupportedRelFromInsertShuttle shuttle = new UnsupportedRelFromInsertShuttle( modify.entity.id );
        modify.accept( shuttle );
        return shuttle.containsOtherTableId;
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        if ( !Objects.equals( scan.getEntity().id, tableId ) ) {
            containsOtherTableId = true;
        }
        return super.visit( scan );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        if ( other instanceof AlgSubset subset ) {
            // we do this for now as ... select from.. is not correctly implemented in this adapter
            // should be picked up by enumerable streamer logic
            if ( subset.getAlgList().size() < 3 ) {
                for ( AlgNode node : subset.getAlgList() ) {
                    node.accept( this );
                }
            }

        }
        return super.visit( other );
    }

}
