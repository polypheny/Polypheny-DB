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

package org.polypheny.db.algebra.core.relational;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Relational operator that returns the contents of a table.
 */
public abstract class RelScan<E extends Entity> extends Scan<E> implements RelAlg {

    protected RelScan( AlgCluster cluster, AlgTraitSet traitSet, @NonNull E entity ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), entity );
    }


    /**
     * Creates a Scan by parsing serialized output.
     */
    protected RelScan( AlgInput input ) {
        this( input.getCluster(), input.getTraitSet(), (E) input.getEntity( "entity" ) );
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return entity.getTupleCount();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = entity.getTupleCount();
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public AlgDataType deriveRowType() {
        if ( entity.dataModel == DataModel.DOCUMENT ) {
            return DocumentType.ofCrossRelational();
        }
        return entity.getTupleType().asRelational();
    }


    /**
     * Returns an identity projection for the given table.
     */
    public static ImmutableList<Integer> identity( Entity entity ) {
        return ImmutableList.copyOf( IntStream.range( 0, entity.getTupleType().getFieldCount() ).boxed().collect( Collectors.toList() ) );
    }


    /**
     * Returns an identity projection.
     */
    public ImmutableList<Integer> identity() {
        return identity( entity );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "table", entity.id )
                .item( "layer", entity.getLayer() );
    }


    /**
     * Projects a subset of the fields of the table, and also asks for "extra" fields that were not included in the table's official type.
     *
     * The default implementation assumes that tables cannot do either of these operations, therefore it adds a {@link Project} that projects {@code NULL} values for the extra fields,
     * using the {@link AlgBuilder#project(Iterable)} method.
     *
     * Subclasses, representing table types that have these capabilities, should override.
     *
     * @param fieldsUsed Bitmap of the fields desired by the consumer
     * @param extraFields Extra fields, not advertised in the table's row-type, wanted by the consumer
     * @param algBuilder Builder used to create a Project
     * @return Relational expression that projects the desired fields
     */
    public AlgNode project( ImmutableBitSet fieldsUsed, Set<AlgDataTypeField> extraFields, AlgBuilder algBuilder ) {
        final int fieldCount = getTupleType().getFieldCount();
        if ( fieldsUsed.equals( ImmutableBitSet.range( fieldCount ) ) && extraFields.isEmpty() ) {
            return this;
        }
        final List<RexNode> exprList = new ArrayList<>();
        final List<String> nameList = new ArrayList<>();
        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final List<AlgDataTypeField> fields = getTupleType().getFields();

        // Project the subset of fields.
        for ( int i : fieldsUsed ) {
            AlgDataTypeField field = fields.get( i );
            exprList.add( rexBuilder.makeInputRef( this, i ) );
            nameList.add( field.getName() );
        }

        // Project nulls for the extra fields. (Maybe a subclass table has extra fields, but we don't.)
        for ( AlgDataTypeField extraField : extraFields ) {
            exprList.add( rexBuilder.ensureType( extraField.getType(), rexBuilder.constantNull(), true ) );
            nameList.add( extraField.getName() );
        }

        return algBuilder.push( this ).project( exprList, nameList ).build();
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                entity.id + "&";
    }


    @Override
    public boolean isCrossModel() {
        return entity.dataModel != DataModel.RELATIONAL;
    }


}
