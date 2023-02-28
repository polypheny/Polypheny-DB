/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.file.algebra.FileScan;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.refactor.ModifiableEntity;
import org.polypheny.db.catalog.refactor.TranslatableEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


public class FileTranslatableEntity extends PhysicalTable implements TranslatableEntity, ModifiableEntity {

    private final File rootDir;
    @Getter
    private final Map<Long, String> columnNames;

    @Getter
    private final Map<String, Long> columnNamesIds;
    @Getter
    private final Map<Long, PolyType> columnTypeMap;
    @Getter
    private final List<Long> pkIds; // Ids of the columns that are part of the primary key
    @Getter
    private final Long adapterId;
    @Getter
    private final FileSchema fileSchema;
    public final AllocationTable allocation;


    public FileTranslatableEntity(
            final FileSchema fileSchema,
            LogicalTable logicalTable, final AllocationTable allocationTable,
            final List<Long> pkIds ) {
        super( allocationTable, logicalTable.name, logicalTable.getNamespaceName(), logicalTable.getColumnNames() );
        this.fileSchema = fileSchema;
        this.rootDir = fileSchema.getRootDir();
        this.adapterId = (long) fileSchema.getAdapterId();
        this.pkIds = pkIds;
        this.allocation = allocationTable;

        this.columnNames = allocationTable.getColumnNames();
        this.columnNamesIds = allocationTable.getColumnNamesIds();
        this.columnTypeMap = allocationTable.getColumns().entrySet().stream().collect( Collectors.toMap( Entry::getKey, a -> a.getValue().type ) );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgTraitSet traitSet ) {
        fileSchema.getConvention().register( context.getCluster().getPlanner() );
        return new FileScan( context.getCluster(), allocation, this );
    }


    @Override
    public Modify<?> toModificationAlg(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            CatalogEntity entity,
            AlgNode child,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList
    ) {
        fileSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                entity,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                true );
    }


}
