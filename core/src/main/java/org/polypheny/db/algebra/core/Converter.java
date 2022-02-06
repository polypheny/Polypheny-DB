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

package org.polypheny.db.algebra.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.logical.LogicalScan;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.serialize.PolySerializer;
import org.polypheny.db.type.PolyType;

@Getter
public class Converter extends AbstractAlgNode {

    private final AlgDataTypeFactory factory;
    private final AlgDataType binaryType;
    protected AlgNode original;
    private final Class<? extends AlgNode> clazz;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param original Input relational expression
     */
    protected Converter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode original ) {
        super( cluster, traits );
        this.original = original;
        this.factory = new JavaTypeFactoryImpl();
        this.binaryType = factory.createPolyType( PolyType.BINARY );
        this.clazz = original.getClass();
    }


    public static boolean containsDeserialize( Project project ) {
        return project.getProjects().stream().anyMatch( e -> e instanceof RexCall && ((RexCall) e).op.getOperatorName() == OperatorName.DESERIALIZE );
    }


    @Override
    public String algCompareString() {
        return "converter$" + getClazz().getSimpleName() + "$" + getOriginal().algCompareString();
    }


    public Values getConvertedInput() {
        Values values = (Values) this.getOriginal();

        List<Integer> replace = getReplacedFields( values.getRowType() );

        return LogicalValues.create( this.getCluster(), getRowType(), convertTuples( values.tuples, replace ) );
    }


    private List<Integer> getReplacedFields( AlgDataType rowType ) {
        List<Integer> replace = new ArrayList<>();
        int i = 0;
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            if ( field.getType().getPolyType() == PolyType.MAP ) {
                replace.add( i );
            }
            i++;
        }
        return replace;
    }


    private ImmutableList<ImmutableList<RexLiteral>> convertTuples( ImmutableList<ImmutableList<RexLiteral>> tuples, List<Integer> replace ) {
        Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();
        for ( ImmutableList<RexLiteral> tuple : tuples ) {
            Builder<RexLiteral> line = ImmutableList.builder();
            int i = 0;
            for ( RexLiteral literal : tuple ) {
                if ( replace.contains( i ) ) {
                    byte[] compressed = PolySerializer.serializeAndCompress( literal.getValue() );
                    line.add( new RexLiteral( new ByteString( compressed ), factory.createPolyType( PolyType.BINARY, compressed.length ), PolyType.BINARY ) );
                } else {
                    line.add( literal );
                }
                i++;
            }

            builder.add( line.build() );
        }

        return builder.build();

    }


    public AlgNode getConvertedScan() {
        Scan scan = (Scan) original;
        AlgOptTable algOptTable = original.getTable();
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        List<Integer> replaced = getReplacedFields( scan.getRowType() );
        AlgDataType rowType = new AlgRecordType( algOptTable.getRowType().getFieldList().stream().map( e -> {
            if ( e.getType().getPolyType() == PolyType.MAP ) {
                return new AlgDataTypeFieldImpl( e.getName(), e.getPhysicalName(), e.getIndex(), factory.createPolyType( PolyType.BINARY, 2024 ) );
            }
            return e;
        } ).collect( Collectors.toList() ) );
        AlgOptTable substitutionTable = AlgOptTableImpl.create( algOptTable.getRelOptSchema(), rowType, algOptTable.getTable(), ImmutableList.copyOf( algOptTable.getQualifiedName() ) );

        assert algOptTable.getTable() instanceof TranslatableTable : "Table to substitute needs to implement TranslatableTable";
        AlgNode newScan = ((TranslatableTable) algOptTable.getTable()).toAlg( scan::getCluster, substitutionTable );

        // create mapping where necessary
        List<RexNode> mappings = new ArrayList<>();
        List<RexNode> first = new ArrayList<>();
        int i = 0;
        for ( AlgDataTypeField field : newScan.getRowType().getFieldList() ) {
            RexNode ref = rexBuilder.makeInputRef( field.getType(), field.getIndex() );
            first.add( ref );
            if ( replaced.contains( i ) ) {
                ref = rexBuilder.makeCall( OperatorRegistry.get( QueryLanguage.MONGO_QL, OperatorName.DESERIALIZE ), ref );
            }

            mappings.add( ref );
            i++;
        }

        return EnumerableProject.create( EnumerableProject.create( newScan, first, newScan.getRowType() ), mappings, scan.getRowType() );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "converter", getOriginal() );
    }


    @Override
    public List<AlgNode> getInputs() {
        if ( getClazz() == LogicalScan.class ) {
            return Collections.singletonList( getOriginal() );
        }
        return super.getInputs();
    }


    @Override
    public void replaceInput( int ordinalInParent, AlgNode alg ) {
        super.replaceInput( ordinalInParent, alg );
        this.original = alg;
    }


    @Override
    protected AlgDataType deriveRowType() {
        if ( getOriginal() instanceof Values ) {
            // we map from unsupported value supported one for Values intern: RowType[ MAP ] -> extern: RowType[ BINARY(2024) ]
            return new AlgRecordType( getOriginal().getRowType().getFieldList().stream().map( f -> {
                if ( f.getType().getPolyType() == PolyType.MAP ) {
                    return new AlgDataTypeFieldImpl( f.getName(), f.getIndex(), getFactory().createPolyType( PolyType.BINARY, 2024 ) );
                }
                return f;
            } ).collect( Collectors.toList() ) );
        }
        return getOriginal().getRowType();

    }


    @Override
    public void register( AlgOptPlanner planner ) {
        getOriginal().getConvention().register( planner );
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( getOriginal(), 0, this );
    }


    public boolean isScan() {
        return getOriginal() instanceof Scan;
    }

}
