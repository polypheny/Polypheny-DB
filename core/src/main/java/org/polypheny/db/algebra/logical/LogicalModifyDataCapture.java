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

package org.polypheny.db.algebra.logical;


import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.replication.ModifyDataCapture;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.replication.cdc.ChangeDataCaptureObject;
import org.polypheny.db.replication.cdc.ChangeDataCollector;
import org.polypheny.db.rex.RexNode;


public class LogicalModifyDataCapture extends ModifyDataCapture {


    @Getter
    private AlgNode input;


    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public LogicalModifyDataCapture(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            long tableId,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            RexNode condition,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions,
            long txId,
            long stmtId,
            AlgNode input ) {
        super( cluster, traitSet, operation, tableId, updateColumnList, sourceExpressionList, condition, fieldList, accessedPartitions, txId, stmtId );
        this.input = input;

        // Already prepares the DataCaptureCollection to be replicated to secondaries
        ChangeDataCollector.prepareCDC(
                ChangeDataCaptureObject.create(
                        txId,
                        stmtId,
                        tableId,
                        operation,
                        updateColumnList,
                        sourceExpressionList,
                        condition,
                        fieldList,
                        getAccessedPartitions()
                ) );
    }


    /**
     * Creates a LogicalModifyReplicator.
     */
    public static LogicalModifyDataCapture create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            long tableId,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            RexNode condition,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions,
            long txId,
            long stmtId,
            AlgNode input ) {
        return new LogicalModifyDataCapture( cluster, traitSet, operation, tableId, updateColumnList, sourceExpressionList, condition, fieldList, accessedPartitions, txId, stmtId, input );
    }


    @Override
    public List<AlgNode> getInputs() {
        return ImmutableList.of( input );
    }


    @Override
    public LogicalModifyDataCapture copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalModifyDataCapture( getCluster(), traitSet, getOperation(), getTableId(), getUpdateColumnList(), getSourceExpressionList(), getCondition(), getFieldList(), getAccessedPartitions(), getTxId(), getStmtId(), sole( inputs ) );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).input( "input", getInput() );
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode alg ) {
        assert ordinalInParent == 0;
        this.input = alg;
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }
}
