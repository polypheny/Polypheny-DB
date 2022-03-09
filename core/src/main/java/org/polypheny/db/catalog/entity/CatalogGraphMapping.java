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

package org.polypheny.db.catalog.entity;

import java.io.Serializable;

public class CatalogGraphMapping implements CatalogObject {

    public final long graphId;

    public final long nodeId;
    public final long edgeId;

    public final long idNodeId;
    public final long nodeNodeId;
    public final long labelsNodeId;

    public final long idEdgeId;
    public final long edgeEdgeId;
    public final long labelsEdgeId;


    public CatalogGraphMapping( long graphId, long nodeId, long edgeId, long idNodeId, long nodeNodeId, long labelsNodeId, long idEdgeId, long edgeEdgeId, long labelsEdgeId ) {
        this.graphId = graphId;
        this.nodeId = nodeId;
        this.edgeId = edgeId;

        this.idNodeId = idNodeId;
        this.nodeNodeId = nodeNodeId;
        this.labelsNodeId = labelsNodeId;

        this.idEdgeId = idEdgeId;
        this.edgeEdgeId = edgeEdgeId;
        this.labelsEdgeId = labelsEdgeId;

    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
