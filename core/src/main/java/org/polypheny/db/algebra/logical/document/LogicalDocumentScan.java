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
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.schema.ModelTrait;


public class LogicalDocumentScan extends DocumentScan implements RelationalTransformable {

    /**
     * Subclass of {@link DocumentScan} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable document ) {
        super( cluster, traitSet, document );
    }


    public static AlgNode create( AlgOptCluster cluster, AlgOptTable collection ) {
        return new LogicalDocumentScan( cluster, cluster.traitSet(), collection );
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        return List.of( AlgOptRule.convert( LogicalScan.create( getCluster(), entities.get( 0 ) ), ModelTrait.RELATIONAL ) );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
