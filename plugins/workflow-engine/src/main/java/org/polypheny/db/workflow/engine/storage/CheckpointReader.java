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

package org.polypheny.db.workflow.engine.storage;

import java.util.Iterator;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.type.entity.PolyValue;

@Getter
public abstract class CheckpointReader {

    private final LogicalEntity entity;


    public CheckpointReader( LogicalEntity entity ) {
        this.entity = entity;
    }


    public abstract AlgNode getAlgNode();

    public abstract Iterator<PolyValue[]> getIterator();

    public abstract Iterator<PolyValue[]> getIteratorFromQuery(String query); // TODO: How to specify query? Query language, PolyAlg or AlgNodes


    public AlgDataType getTupleType() {
        return entity.getTupleType();
    }

}
