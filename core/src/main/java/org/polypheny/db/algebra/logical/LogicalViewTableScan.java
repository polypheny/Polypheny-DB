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

package org.polypheny.db.algebra.logical;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.Table;

public class LogicalViewTableScan extends TableScan {

    @Getter
    private final AlgNode algNode;
    @Getter
    private final AlgCollation relCollation;


    public LogicalViewTableScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, AlgNode algNode, AlgCollation algCollation ) {
        super( cluster, traitSet, table );
        this.algNode = algNode;
        this.relCollation = algCollation;
    }


    public static AlgNode create( AlgOptCluster cluster, final AlgOptTable relOptTable ) {
        final Table table = relOptTable.unwrap( Table.class );

        final AlgTraitSet traitSet =
                cluster.traitSetOf( Convention.NONE )
                        .replaceIfs(
                                AlgCollationTraitDef.INSTANCE,
                                () -> {
                                    if ( table != null ) {
                                        return table.getStatistic().getCollations();
                                    }
                                    return ImmutableList.of();
                                } );

        Catalog catalog = Catalog.getInstance();

        long idLogical = ((LogicalTable) ((AlgOptTableImpl) relOptTable).getTable()).getTableId();
        CatalogTable catalogTable = catalog.getTable( idLogical );

        return new LogicalViewTableScan( cluster, traitSet, relOptTable, ((CatalogView) catalogTable).prepareView( cluster ), ((CatalogView) catalogTable).getAlgCollation() );
    }

}
