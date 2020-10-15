/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.file.rel;


import java.util.List;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexNode;


public class FileTableModify extends TableModify implements FileRel {

    public FileTableModify( RelOptCluster cluster, RelTraitSet traits, RelOptTable table, CatalogReader catalogReader, RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traits, table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }

    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new FileTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                AbstractRelNode.sole( inputs ),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }

    @Override
    public void register( RelOptPlanner planner ) {
        getConvention().register( planner );
    }

    @Override
    public void implement( FileImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        FileTranslatableTable fileTable = (FileTranslatableTable) ((RelOptTableImpl) getTable()).getTable();
        implementor.setFileTable( fileTable );
        Operation operation = getOperation();
        switch ( operation ) {
            case INSERT:
                implementor.setOperation( FileImplementor.Operation.INSERT );
                break;
            case UPDATE:
                implementor.setOperation( FileImplementor.Operation.UPDATE );
                break;
            case DELETE:
                implementor.setOperation( FileImplementor.Operation.DELETE );
                break;
            default:
                throw new RuntimeException( "The File adapter does not support " + operation + "operations." );
        }
    }
}
