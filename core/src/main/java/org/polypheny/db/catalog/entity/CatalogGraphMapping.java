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

    private static final long serialVersionUID = -4430656159805278460L;

    public final long id;

    public final long nodesId;
    public final long idNodeId;
    public final long labelNodeId;

    public final long nodesPropertyId;
    public final long idNodesPropertyId;
    public final long keyNodePropertyId;
    public final long valueNodePropertyId;

    public final long edgesId;
    public final long idEdgeId;
    public final long labelEdgeId;
    public final long sourceEdgeId;
    public final long targetEdgeId;

    public final long idEdgesPropertyId;
    public final long edgesPropertyId;
    public final long keyEdgePropertyId;
    public final long valueEdgePropertyId;


    public CatalogGraphMapping(
            long id,
            long nodesId,
            long idNodeId,
            long labelNodeId,
            long nodesPropertyId,
            long idNodesPropertyId,
            long keyNodePropertyId,
            long valueNodePropertyId,
            long edgesId,
            long idEdgeId,
            long labelEdgeId,
            long sourceEdgeId,
            long targetEdgeId,
            long edgesPropertyId,
            long idEdgesPropertyId,
            long keyEdgePropertyId,
            long valueEdgePropertyId ) {
        this.id = id;
        this.nodesId = nodesId;
        this.idNodeId = idNodeId;
        this.labelNodeId = labelNodeId;

        this.nodesPropertyId = nodesPropertyId;
        this.idNodesPropertyId = idNodesPropertyId;
        this.keyNodePropertyId = keyNodePropertyId;
        this.valueNodePropertyId = valueNodePropertyId;

        this.edgesId = edgesId;
        this.idEdgeId = idEdgeId;
        this.labelEdgeId = labelEdgeId;
        this.sourceEdgeId = sourceEdgeId;
        this.targetEdgeId = targetEdgeId;

        this.idEdgesPropertyId = idEdgesPropertyId;
        this.edgesPropertyId = edgesPropertyId;
        this.keyEdgePropertyId = keyEdgePropertyId;
        this.valueEdgePropertyId = valueEdgePropertyId;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


}
