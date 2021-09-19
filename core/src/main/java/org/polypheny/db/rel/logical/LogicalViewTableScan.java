/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.rel.logical;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.Table;

public class LogicalViewTableScan extends TableScan {

    @Getter
    private final RelNode relNode;
    @Getter
    private final RelCollation relCollation;


    public LogicalViewTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RelNode relNode, RelCollation relCollation ) {
        super( cluster, traitSet, table );
        this.relNode = relNode;
        this.relCollation = relCollation;
    }


    public static RelNode create( RelOptCluster cluster, final RelOptTable relOptTable ) {
        final Table table = relOptTable.unwrap( Table.class );

        final RelTraitSet traitSet =
                cluster.traitSetOf( Convention.NONE )
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> {
                                    if ( table != null ) {
                                        return table.getStatistic().getCollations();
                                    }
                                    return ImmutableList.of();
                                } );

        Catalog catalog = Catalog.getInstance();

        long idLogical = ((LogicalTable) ((RelOptTableImpl) relOptTable).getTable()).getTableId();
        CatalogTable catalogTable = catalog.getTable( idLogical );

        return new LogicalViewTableScan( cluster, traitSet, relOptTable, ((CatalogView) catalogTable).prepareView( cluster ), ((CatalogView) catalogTable).getRelCollation() );
    }

}
