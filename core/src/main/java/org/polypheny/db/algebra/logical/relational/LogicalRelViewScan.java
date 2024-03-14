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

package org.polypheny.db.algebra.logical.relational;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;


@Getter
public class LogicalRelViewScan extends RelScan<Entity> {

    private final AlgNode algNode;
    private final AlgCollation algCollation;


    public LogicalRelViewScan( AlgCluster cluster, AlgTraitSet traitSet, Entity table, AlgNode algNode, AlgCollation algCollation ) {
        super( cluster, traitSet, table );
        this.algNode = algNode;
        this.algCollation = algCollation;
    }


    public static AlgNode create( AlgCluster cluster, final Entity entity ) {

        final AlgTraitSet traitSet =
                cluster.traitSetOf( Convention.NONE )
                        .replaceIfs(
                                AlgCollationTraitDef.INSTANCE,
                                () -> {
                                    if ( entity != null ) {
                                        return entity.getCollations();
                                    }
                                    return ImmutableList.of();
                                } );

        LogicalView logicalView = entity.unwrap( LogicalView.class ).orElseThrow();
        AlgCollation algCollation = logicalView.getAlgCollation();

        return new LogicalRelViewScan( cluster, traitSet, entity, logicalView.prepareView( cluster ), algCollation );
    }


    @Override
    public boolean containsView() {
        return true;
    }


    @Override
    public AlgNode unfoldView( @Nullable AlgNode parent, int index, AlgCluster cluster ) {
        AlgNode unfolded = unfoldView( cluster ).unfoldView( this, 0, cluster );
        if ( parent != null ) {
            parent.replaceInput( index, unfolded );
        }
        return unfolded;
    }


    public AlgNode unfoldView( AlgCluster cluster ) {
        RexBuilder rexBuilder = this.getCluster().getRexBuilder();
        final List<RexNode> exprs = new ArrayList<>();
        final AlgDataType rowType = this.getTupleType();
        final int fieldCount = rowType.getFieldCount();
        for ( int i = 0; i < fieldCount; i++ ) {
            exprs.add( rexBuilder.makeInputRef( this, i ) );
        }

        algNode.replaceCluster( cluster );

        return LogicalRelProject.create( algNode, exprs, this.getTupleType().getFieldNames() );
    }

}
