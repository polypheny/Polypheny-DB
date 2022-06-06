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

package org.polypheny.db.algebra.core.document;

import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;

public class DocumentModify extends SingleAlg implements DocumentAlg {

    public final Operation operation;
    @Getter
    private final AlgOptTable table;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param table
     * @param traits
     * @param input Input relational expression
     * @param operation
     */
    protected DocumentModify( AlgOptCluster cluster, AlgOptTable table, AlgTraitSet traits, AlgNode input, Operation operation ) {
        super( cluster, traits, input );
        this.operation = operation;
        this.table = table;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + input.algCompareString();
    }


    @Override
    public DocType getDocType() {
        return DocType.MODIFY;
    }

}
