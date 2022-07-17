/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.rndschema;

import java.util.Set;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.type.PolyType;

public class ColumnNode {
    public TableNode tableNode;

    public String columnName = null;
    public PolyType polyType = null;
    public PolyType collectionType = null;
    public Integer precision = null;
    public Integer scale = null;
    public Integer dimension = null;
    public Integer cardinality = null;
    public Boolean nullable = null;
    public Collation collation = null;
    public String defaultValue = null;

    public long columnId;

    public ColumnNode references;
    public Set<ColumnNode> referencedBy;
}