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

package org.polypheny.db.algebra.core.relational;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;


/**
 * Relational operator that returns the contents of a table.
 */
public abstract class RelScan<E extends CatalogEntity> extends Scan<E> {

    /**
     * The entity definition.
     */
    public final E entity;


    protected RelScan( AlgOptCluster cluster, AlgTraitSet traitSet, @NonNull E entity ) {
        super( cluster, traitSet, entity );
        this.entity = entity;
    }


    /**
     * Creates a Scan by parsing serialized output.
     */
    protected RelScan( AlgInput input ) {
        this( input.getCluster(), input.getTraitSet(), (E) input.getEntity( "entity" ) );
    }


    @Override
    public double estimateRowCount( AlgMetadataQuery mq ) {
        return entity.getRowCount();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        double dRows = entity.getRowCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public AlgDataType deriveRowType() {
        return entity.getRowType();
    }


    /**
     * Returns an identity projection for the given table.
     */
    public static ImmutableIntList identity( CatalogEntity entity ) {
        return ImmutableIntList.identity( entity.getRowType().getFieldCount() );
    }


    /**
     * Returns an identity projection.
     */
    public ImmutableIntList identity() {
        return identity( entity );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "table", id );
    }


    /**
     * Projects a subset of the fields of the table, and also asks for "extra" fields that were not included in the table's official type.
     *
     * The default implementation assumes that tables cannot do either of these operations, therefore it adds a {@link Project} that projects {@code NULL} values for the extra fields,
     * using the {@link AlgBuilder#project(Iterable)} method.
     *
     * Sub-classes, representing table types that have these capabilities, should override.
     *
     * @param fieldsUsed Bitmap of the fields desired by the consumer
     * @param extraFields Extra fields, not advertised in the table's row-type, wanted by the consumer
     * @param algBuilder Builder used to create a Project
     * @return Relational expression that projects the desired fields
     */
    public AlgNode project( ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields, AlgBuilder algBuilder ) {
        final int fieldCount = getRowType().getFieldCount();
        if ( fieldsUsed.equals( ImmutableBitSet.range( fieldCount ) ) && extraFields.isEmpty() ) {
            return this;
        }
        final List<RexNode> exprList = new ArrayList<>();
        final List<String> nameList = new ArrayList<>();
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final List<AlgDataTypeField> fields = getRowType().getFieldList();

        // Project the subset of fields.
        for ( int i : fieldsUsed ) {
            AlgDataTypeField field = fields.get( i );
            exprList.add( rexBuilder.makeInputRef( this, i ) );
            nameList.add( field.getName() );
        }

        // Project nulls for the extra fields. (Maybe a sub-class table has extra fields, but we don't.)
        for ( AlgDataTypeField extraField : extraFields ) {
            exprList.add( rexBuilder.ensureType( extraField.getType(), rexBuilder.constantNull(), true ) );
            nameList.add( extraField.getName() );
        }

        return algBuilder.push( this ).project( exprList, nameList ).build();
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                entity.id + "&";
    }

}
