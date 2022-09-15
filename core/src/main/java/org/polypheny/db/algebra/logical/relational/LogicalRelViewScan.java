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

package org.polypheny.db.algebra.logical.relational;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.Table;


public class LogicalRelViewScan extends Scan {

    @Getter
    private final AlgNode algNode;
    @Getter
    private final AlgCollation algCollation;


    public LogicalRelViewScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, AlgNode algNode, AlgCollation algCollation ) {
        super( cluster, traitSet, table );
        this.algNode = algNode;
        this.algCollation = algCollation;
    }


    public static AlgNode create( AlgOptCluster cluster, final AlgOptTable algOptTable ) {
        final Table table = algOptTable.unwrap( Table.class );

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

        long idLogical = ((LogicalTable) algOptTable.getTable()).getTableId();
        CatalogTable catalogTable = catalog.getTable( idLogical );
        AlgCollation algCollation = ((CatalogView) catalogTable).getAlgCollation();

        return new LogicalRelViewScan( cluster, traitSet, algOptTable, ((CatalogView) catalogTable).prepareView( cluster ), algCollation );
    }


    @Override
    public boolean hasView() {
        return true;
    }


    public AlgNode expandViewNode() {
        RexBuilder rexBuilder = this.getCluster().getRexBuilder();
        final List<RexNode> exprs = new ArrayList<>();
        final AlgDataType rowType = this.getRowType();
        final int fieldCount = rowType.getFieldCount();
        for ( int i = 0; i < fieldCount; i++ ) {
            exprs.add( rexBuilder.makeInputRef( this, i ) );
        }

        return LogicalProject.create( algNode, exprs, this.getRowType().getFieldNames() );
    }

}
