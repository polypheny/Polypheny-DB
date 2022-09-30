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
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;


public class LogicalDocumentModify extends DocumentModify implements RelationalTransformable {

    /**
     * Subclass of {@link DocumentModify} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentModify( AlgTraitSet traits, AlgOptTable table, CatalogReader catalogReader, AlgNode input, Operation operation, List<String> keys, List<RexNode> updates ) {
        super( traits, table, catalogReader, input, operation, keys, updates );
    }


    public static LogicalDocumentModify create( AlgOptTable table, AlgNode input, CatalogReader catalogReader, Operation operation, List<String> keys, List<RexNode> updates ) {
        return new LogicalDocumentModify( input.getTraitSet(), table, catalogReader, input, operation, keys, updates );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentModify( traitSet, getCollection(), getCatalogReader(), inputs.get( 0 ), operation, getKeys(), getUpdates() );
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        return List.of( RelationalTransformable.getModify( entities.get( 0 ), catalogReader, values.get( 0 ), operation ) );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
