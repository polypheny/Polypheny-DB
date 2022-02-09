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
import java.util.List;
import lombok.Getter;
import org.apache.calcite.avatica.util.ByteString;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.serialize.PolySerializer;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;

@Getter
public class Transformer extends SingleAlg {

    private final AlgDataTypeFactory factory;

    private final List<PolyType> unsupportedTypes;
    private final PolyType substituteType;


    // BINARY would be better, but is not supported by most SQL based stores,
    // which again would require substitution, maybe change later
    public static final int substitutionLength = 2024;


    /**
     * {@link Transformer} is used to signal the underlying stores that a non-supported type is encountered,
     * which either needs adjustments of the dml operation itself or requires a transformation after retrieval of a dql scan
     * DQL operations can be ignored by the stores, as they are most of the time handled by the EnumerableTransformer after the store supported scan
     * DML operations need adjustment of the operation itself.
     *
     * e.g. TableModify -> Values
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param rowType
     * @param unsupportedTypes
     * @param substituteType
     */
    protected Transformer( AlgOptCluster cluster, AlgTraitSet traits, AlgDataType rowType, AlgNode input, List<PolyType> unsupportedTypes, PolyType substituteType ) {
        super( cluster, traits, input );
        this.factory = new JavaTypeFactoryImpl();
        this.unsupportedTypes = unsupportedTypes;
        this.substituteType = substituteType;
        this.rowType = rowType;
    }


    public static boolean containsDeserialize( Project project ) {
        return project.getProjects().stream().anyMatch( e -> e instanceof RexCall && ((RexCall) e).op.getOperatorName() == OperatorName.DESERIALIZE );
    }


    @Override
    public String algCompareString() {
        return "converter$" + getInput().algCompareString();
    }


    public static LogicalValues getConvertedInput( LogicalValues values, AlgDataType targetRowType, PolyType substitutionType, AlgDataTypeFactory factory ) {

        List<Integer> replace = getReplacedFields( values.getRowType() );

        return LogicalValues.create( values.getCluster(), targetRowType, convertTuples( factory, values.tuples, replace, substitutionType ) );
    }


    protected static List<Integer> getReplacedFields( AlgDataType rowType ) {
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


    private static ImmutableList<ImmutableList<RexLiteral>> convertTuples( AlgDataTypeFactory factory, ImmutableList<ImmutableList<RexLiteral>> tuples, List<Integer> replace, PolyType substitutionType ) {
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


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "converter", input );
    }


    public enum SubstitutionType {
        SCAN,
        VALUES
    }

}
