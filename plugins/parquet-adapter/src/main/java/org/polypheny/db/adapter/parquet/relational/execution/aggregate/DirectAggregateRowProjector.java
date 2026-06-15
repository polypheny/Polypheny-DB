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

package org.polypheny.db.adapter.parquet.relational.execution.aggregate;

import java.util.Arrays;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Projects aggregate input directly from row fields.
 */
public final class DirectAggregateRowProjector implements AggregateRowProjector {

    private final int[] groupFields;


    public DirectAggregateRowProjector( int[] groupFields ) {
        this.groupFields = Arrays.copyOf( groupFields, groupFields.length );
    }


    @Override
    public boolean accepts( PolyValue[] row ) {
        return true;
    }


    @Override
    public GroupKey groupKey( PolyValue[] row ) {
        if ( groupFields.length == 0 ) {
            return GroupKey.Empty;
        }
        Object[] key = new Object[groupFields.length];
        for ( int i = 0; i < groupFields.length; i++ ) {
            key[i] = row[groupFields[i]];
        }
        return GroupKey.of( key );
    }


    @Override
    public void add( AggregateGroupState values, PolyValue[] row ) {
        values.add( row );
    }

}
