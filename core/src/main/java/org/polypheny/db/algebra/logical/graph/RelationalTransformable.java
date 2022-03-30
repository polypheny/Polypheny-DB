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

package org.polypheny.db.algebra.logical.graph;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;

public interface RelationalTransformable {

    default AlgOptTable getNodeTable() {
        throw new UnsupportedOperationException();
    }

    default void setNodeTable( AlgOptTable table ) {
        throw new UnsupportedOperationException();
    }

    default AlgOptTable getNodePropertyTable() {
        throw new UnsupportedOperationException();
    }

    default void setNodePropertyTable( AlgOptTable table ) {
        throw new UnsupportedOperationException();
    }

    default AlgOptTable getEdgeTable() {
        throw new UnsupportedOperationException();
    }

    default void setEdgeTable( AlgOptTable table ) {
        throw new UnsupportedOperationException();
    }

    default AlgOptTable getEdgePropertyTable() {
        throw new UnsupportedOperationException();
    }

    default void setEdgePropertyTable( AlgOptTable table ) {
        throw new UnsupportedOperationException();
    }

    default CatalogReader getCatalogReader() {
        throw new UnsupportedOperationException();
    }


    List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities );

    boolean canTransform();

}
