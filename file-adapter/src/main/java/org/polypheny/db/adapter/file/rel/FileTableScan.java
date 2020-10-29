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
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRelImplementor;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;


public class FileTableScan extends TableScan implements FileRel {

    private final FileTranslatableTable fileTable;

    public FileTableScan ( RelOptCluster cluster, RelOptTable table, FileTranslatableTable fileTable ) {
        //convention was: EnumerableConvention.INSTANCE
        super( cluster, cluster.traitSetOf( fileTable.getFileSchema().getConvention() ), table );
        this.fileTable = fileTable;
    }

    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new FileTableScan( getCluster(), table, fileTable );
    }

    @Override
    public RelDataType deriveRowType() {
        return fileTable.getRowType( getCluster().getTypeFactory() );
    }

    @Override
    public void register( RelOptPlanner planner ) {
        getConvention().register( planner );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }

    @Override
    public void implement( FileImplementor implementor ) {
        implementor.setFileTable( fileTable );
        //only set SELECT operation if we're not in a insert/update/delete
        if ( implementor.getOperation() == null ) {
            implementor.setOperation( Operation.SELECT );
        }
    }
}
