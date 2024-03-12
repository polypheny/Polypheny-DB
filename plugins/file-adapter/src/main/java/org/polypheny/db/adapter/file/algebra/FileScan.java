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

package org.polypheny.db.adapter.file.algebra;


import java.util.List;
import org.polypheny.db.adapter.file.FileAlg;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;


public class FileScan extends RelScan<FileTranslatableEntity> implements FileAlg {

    private final FileTranslatableEntity fileTable;


    public FileScan( AlgCluster cluster, Entity table, FileTranslatableEntity fileTable ) {
        //convention was: EnumerableConvention.INSTANCE
        super( cluster, cluster.traitSetOf( fileTable.getFileSchema().getConvention() ).replace( ModelTrait.RELATIONAL ), fileTable );
        this.fileTable = fileTable;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new FileScan( getCluster(), entity, fileTable );
    }


    @Override
    public AlgDataType deriveRowType() {
        return fileTable.getTupleType();
    }


    @Override
    public void register( AlgPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
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
