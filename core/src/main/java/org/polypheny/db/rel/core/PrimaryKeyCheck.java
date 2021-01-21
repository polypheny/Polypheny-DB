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

package org.polypheny.db.rel.core;


import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;


public abstract class PrimaryKeyCheck extends AbstractRelNode {

    protected RelNode dbSource;
    protected RelNode values;
    protected RelOptTable table;


    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public PrimaryKeyCheck( RelNode dbSource, RelNode values, RelOptTable table ) {
        super( dbSource.getCluster(), dbSource.getTraitSet() );
        this.dbSource = dbSource;
        this.values = values;
        this.table = table;
    }

}
