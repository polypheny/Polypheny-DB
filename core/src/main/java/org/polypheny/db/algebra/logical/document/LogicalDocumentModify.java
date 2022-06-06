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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;

public class LogicalDocumentModify extends DocumentModify implements RelationalTransformable {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param table
     * @param traits
     * @param input Input relational expression
     */
    protected LogicalDocumentModify( AlgOptCluster cluster, AlgOptTable table, AlgTraitSet traits, AlgNode input, Operation operation ) {
        super( cluster, table, traits, input, operation );
    }


    public static AlgNode create( AlgOptTable table, AlgNode input, Operation operation ) {
        return new LogicalDocumentModify( input.getCluster(), table, input.getTraitSet(), input, operation );
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        return List.of( RelationalTransformable.getModify( entities.get( 0 ), catalogReader, values.get( 0 ), operation ) );
    }

}
