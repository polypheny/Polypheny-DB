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

package org.polypheny.db.algebra.replication;


import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


public abstract class ModifyDataCapture extends AbstractAlgNode {

    @Getter
    private final Operation operation;

    @Getter
    protected final long tableId;

    @Getter
    private final List<String> updateColumnList;
    @Getter
    private final List<RexNode> sourceExpressionList;

    @Getter
    private final List<AlgDataTypeField> fieldList;

    @Getter
    private final List<Long> accessedPartitions;

    @Getter
    private final long txId;

    @Getter
    private final long stmtId;


    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public ModifyDataCapture(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            long tableId,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions,
            long txId,
            long stmtId ) {
        super( cluster, traitSet );
        this.operation = operation;
        this.tableId = tableId;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
        this.fieldList = fieldList;
        this.accessedPartitions = accessedPartitions;
        this.txId = txId;
        this.stmtId = stmtId;
    }


    @Override
    public AlgDataType deriveRowType() {
        return AlgOptUtil.createDmlRowType( Kind.INSERT, getCluster().getTypeFactory() );
    }


    /**
     * Returns a string which allows to compare alg plans.
     */
    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$";
    }
}
