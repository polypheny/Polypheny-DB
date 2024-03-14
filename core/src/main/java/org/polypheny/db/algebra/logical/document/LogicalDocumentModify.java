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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import java.util.Map;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

@SuperBuilder(toBuilder = true)
public class LogicalDocumentModify extends DocumentModify<Entity> implements RelationalTransformable {

    /**
     * Subclass of {@link DocumentModify} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentModify( AlgTraitSet traits, Entity entity, AlgNode input, Operation operation, Map<String, ? extends RexNode> updates, List<String> removes, Map<String, String> renames ) {
        super( traits, entity, input, operation, updates, removes, renames );
    }


    public static LogicalDocumentModify create( Entity entity, AlgNode input, Operation operation, Map<String, ? extends RexNode> updates, List<String> removes, Map<String, String> renames ) {
        return new LogicalDocumentModify( input.getTraitSet(), entity, input, operation, updates, removes, renames );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentModify( traitSet, entity, inputs.get( 0 ), operation, updates, removes, renames );
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<Entity> entities, Snapshot snapshot ) {
        return List.of( RelationalTransformable.getModify( entities.get( 0 ), values.get( 0 ), operation ) );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
