/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.partition.raw;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;


@Getter
@Setter
public class RawPartitionInformation {

    public SqlIdentifier partitionColumn;
    public SqlIdentifier partitionType;

    public List<SqlIdentifier> partitionNamesList;
    public List<List<SqlNode>> partitionQualifierList;

    public long numPartitionGroups;
    public long numPartitions;

}
