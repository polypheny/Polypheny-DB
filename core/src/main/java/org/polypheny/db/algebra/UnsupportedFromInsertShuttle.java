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

package org.polypheny.db.algebra;

import java.util.Objects;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.plan.volcano.AlgSubset;

public class UnsupportedFromInsertShuttle extends AlgShuttleImpl {

    private final Long tableId;
    private boolean containsOtherTableId = false;


    private UnsupportedFromInsertShuttle( Long tableId ) {
        this.tableId = tableId;
    }


    public static boolean contains( RelModify modify ) {
        long id = modify.getEntity().getCatalogEntity().id;
        UnsupportedFromInsertShuttle shuttle = new UnsupportedFromInsertShuttle( id );
        modify.accept( shuttle );
        return shuttle.containsOtherTableId;
    }


    @Override
    public AlgNode visit( RelScan scan ) {
        if ( !Objects.equals( scan.getEntity().getCatalogEntity().id, tableId ) ) {
            containsOtherTableId = true;
        }
        return super.visit( scan );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        if ( other instanceof AlgSubset ) {
            AlgSubset subset = (AlgSubset) other;
            // we do this for now as ... select from.. is not correctly implemented in this adapter, todo fix and remove
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
