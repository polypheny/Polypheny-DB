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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.file.algebra.FileScan;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.PolyType;


public class FileTranslatableEntity extends PhysicalTable implements TranslatableEntity, ModifiableTable {

    private final File rootDir;
    @Getter
    private final Map<Long, String> columnIdNames;

    @Getter
    private final Map<String, Long> columnNamesIds;
    @Getter
    private final Map<Long, PolyType> columnTypeMap;
    @Getter
    private final List<Long> pkIds; // Ids of the columns that are part of the primary key
    @Getter
    private final FileSchema fileSchema;
    private final PhysicalTable physical;


    public FileTranslatableEntity(
            final FileSchema fileSchema,
            PhysicalTable physical,
            final List<Long> pkIds ) {
        super( physical.id,
                physical.allocationId,
                physical.logicalId,
                physical.name,
                physical.columns,
                physical.namespaceId,
                physical.namespaceName,
                physical.uniqueFieldIds,
                physical.adapterId );
        this.fileSchema = fileSchema;
        this.rootDir = fileSchema.getRootDir();
        this.pkIds = pkIds;
        this.physical = physical;

        this.columnIdNames = physical.columns.stream().collect( Collectors.toMap( p -> p.id, p -> p.name ) );
        this.columnNamesIds = physical.columns.stream().collect( Collectors.toMap( p -> p.name, p -> p.id ) );
        this.columnTypeMap = physical.columns.stream().collect( Collectors.toMap( p -> p.id, p -> p.type ) );
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        fileSchema.getConvention().register( cluster.getPlanner() );
        return new FileScan( cluster, physical, this );
    }


    @Override
    public Modify<?> toModificationTable(
            AlgCluster cluster,
            AlgTraitSet algTraits,
            Entity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList ) {
        fileSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                input,
                operation,
                updateColumnList,
                sourceExpressionList,
                true );
    }


}
