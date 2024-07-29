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

package org.polypheny.db.partition;

import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.type.PolyType;

public class NonePartitionManager extends AbstractPartitionManager {

    @Override
    public boolean requiresUnboundPartitionGroup() {
        return false;
    }


    @Override
    public boolean supportsColumnOfType( PolyType type ) {
        return false;
    }


    @Override
    public long getTargetPartitionId( LogicalTable table, PartitionProperty property, String columnValue ) {
        return property.partitionIds.get( 0 );
    }


    @Override
    public PartitionFunctionInfo getPartitionFunctionInfo() {
        throw new NotImplementedException();
    }

}
