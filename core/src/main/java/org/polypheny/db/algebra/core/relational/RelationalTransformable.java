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

package org.polypheny.db.algebra.core.relational;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.schema.types.ModifiableTable;


/**
 * This interface marks classes, which can be transformed to relational ones.
 */
public interface RelationalTransformable {


    List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<Entity> entities, Snapshot snapshot );


    static Modify<?> getModify( Entity entity, AlgNode alg, Operation operation ) {
        return entity.unwrap( ModifiableTable.class ).orElseThrow().toModificationTable( alg.getCluster(), alg.getTraitSet(), entity, alg, operation, null, null );
    }

}
