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
    public final long relId;

    public final long idNodeId;
    public final long nodeNodeId;
    public final long labelsNodeId;

    public final long idRelId;
    public final long relRelId;
    public final long labelsRelId;


    public CatalogGraphMapping( long graphId, long nodeId, long relId, long idNodeId, long nodeNodeId, long labelsNodeId, long idRelId, long relRelId, long labelsRelId ) {
        this.graphId = graphId;
        this.nodeId = nodeId;
        this.relId = relId;

        this.idNodeId = idNodeId;
        this.nodeNodeId = nodeNodeId;
        this.labelsNodeId = labelsNodeId;

        this.idRelId = idRelId;
        this.relRelId = relRelId;
        this.labelsRelId = labelsRelId;

    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }

}
