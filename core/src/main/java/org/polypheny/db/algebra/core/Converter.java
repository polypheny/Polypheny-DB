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
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.logical.LogicalScan;
import org.polypheny.db.algebra.logical.LogicalTableModify;
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
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;

@Getter
public class Converter extends AbstractAlgNode {

    private final AlgDataTypeFactory factory;
    protected AlgNode original;
    private final Class<? extends AlgNode> clazz;

    // BINARY would be better, but is not supported by most SQL based stores,
    // which again would require substitution, maybe change later
    public static final PolyType substitutionType = PolyType.VARCHAR;
    public static final int substitutionLength = 2024;


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
        this.clazz = original.getClass();
    }


    public static boolean containsDeserialize( Project project ) {
        return project.getProjects().stream().anyMatch( e -> e instanceof RexCall && ((RexCall) e).op.getOperatorName() == OperatorName.DESERIALIZE );
    }


    @Override
    public String algCompareString() {
        return "converter$" + getClazz().getSimpleName() + "$" + getOriginal().algCompareString();
    }


    public AlgNode getTransformed() {
        return getOriginal().accept( new Transformer( factory, getRowType() ) );
    }


    public static Values getConvertedInput( Values values, AlgDataType targetRowType, AlgDataTypeFactory factory ) {

        List<Integer> replace = getReplacedFields( values.getRowType() );

        return LogicalValues.create( values.getCluster(), targetRowType, convertTuples( factory, values.tuples, replace ) );
    }


    private static List<Integer> getReplacedFields( AlgDataType rowType ) {
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


    private static ImmutableList<ImmutableList<RexLiteral>> convertTuples( AlgDataTypeFactory factory, ImmutableList<ImmutableList<RexLiteral>> tuples, List<Integer> replace ) {
        Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();
        for ( ImmutableList<RexLiteral> tuple : tuples ) {
            Builder<RexLiteral> line = ImmutableList.builder();
            int i = 0;
            for ( RexLiteral literal : tuple ) {
                if ( replace.contains( i ) ) {
                    byte[] compressed = PolySerializer.serializeAndCompress( literal.getValue() );
                    line.add( new RexLiteral( new NlsString( new ByteString( compressed ).toBase64String(), "UTF8", Collation.IMPLICIT ), factory.createPolyType( substitutionType, compressed.length ), PolyType.CHAR ) );
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
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();
        List<Integer> replaced = getReplacedFields( scan.getRowType() );
        AlgNode newScan = createSubstitutedScan( scan, factory );

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


    private static AlgNode createSubstitutedScan( Scan scan, AlgDataTypeFactory factory ) {
        AlgOptTable algOptTable = scan.getTable();
        AlgDataType rowType = new AlgRecordType( algOptTable.getRowType().getFieldList().stream().map( e -> {
            if ( e.getType().getPolyType() == PolyType.MAP ) {
                return new AlgDataTypeFieldImpl( e.getName(), e.getPhysicalName(), e.getIndex(), factory.createPolyType( substitutionType, substitutionLength ) );
            }
            return e;
        } ).collect( Collectors.toList() ) );
        AlgOptTable substitutionTable = ((AlgOptTableImpl) scan.getTable()).copy( rowType );

        assert algOptTable.getTable() instanceof TranslatableTable : "Table to substitute needs to implement TranslatableTable";
        return ((TranslatableTable) algOptTable.getTable()).toAlg( scan::getCluster, substitutionTable );
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
                    return new AlgDataTypeFieldImpl( f.getName(), f.getIndex(), getFactory().createPolyType( substitutionType, substitutionLength ) );
                }
                return f;
            } ).collect( Collectors.toList() ) );
        }
        return getOriginal().getRowType();

    }


    @Override
    public void register( AlgOptPlanner planner ) {
        getOriginal().accept( new SubstitutionRegisterer( planner ) );
    }


    @Override
    public void childrenAccept( AlgVisitor visitor ) {
        visitor.visit( getOriginal(), 0, this );
    }


    public boolean isScan() {
        return getOriginal() instanceof Scan;
    }


    public enum SubstitutionType {
        SCAN,
        VALUES
    }


    private static class SubstitutionRegisterer extends AlgShuttleImpl {

        private final AlgOptPlanner planner;


        public SubstitutionRegisterer( AlgOptPlanner planner ) {
            this.planner = planner;
        }


        @Override
        public AlgNode visit( Scan scan ) {
            scan.getConvention().register( planner );
            return super.visit( scan );
        }

    }


    private static class Transformer extends AlgShuttleImpl {

        private final AlgDataTypeFactory factory;
        private final AlgDataType targetRowType;


        public Transformer( AlgDataTypeFactory factory, AlgDataType targetRowType ) {
            this.factory = factory;
            this.targetRowType = targetRowType;
        }


        @Override
        public AlgNode visit( LogicalValues values ) {
            return getConvertedInput( values, targetRowType, factory );
        }


        @Override
        public AlgNode visit( Scan scan ) {
            return createSubstitutedScan( scan, factory );
        }


        @Override
        public AlgNode visit( AlgNode other ) {
            TableModify node = (TableModify) super.visit( other );
            if ( other instanceof TableModify ) {
                return LogicalTableModify.create( node.getInput().getTable(), node.getCatalogReader(), node.getInput(), node.getOperation(), node.getUpdateColumnList(), node.getSourceExpressionList(), node.isFlattened() );
            }
            return node;
        }

    }

}
