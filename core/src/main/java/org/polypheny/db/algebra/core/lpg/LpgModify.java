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

package org.polypheny.db.algebra.core.lpg;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.document.PolyString;


public abstract class LpgModify<E extends CatalogEntity> extends Modify<E> implements LpgAlg {

    @Getter
    public final Operation operation;
    public final List<PolyString> ids;
    public final List<? extends RexNode> operations;


    /**
     * Creates a {@link LpgModify}.
     * {@link ModelTrait#GRAPH} node, which is able to modify an LPG graph.
     */
    protected LpgModify( AlgOptCluster cluster, AlgTraitSet traits, E graph, AlgNode input, Operation operation, List<PolyString> ids, List<? extends RexNode> operations, AlgDataType dmlRowType ) {
        super( cluster, traits.replace( ModelTrait.GRAPH ), graph, input );
        this.operation = operation;
        this.ids = ids;
        this.operations = operations;
        this.rowType = dmlRowType;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() +
                "$" + (ids != null ? ids.hashCode() : "[]") +
                "$" + (operations != null ? operations.hashCode() : "[]") +
                "{" + input.algCompareString() + "}";
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.MODIFY;
    }


}
