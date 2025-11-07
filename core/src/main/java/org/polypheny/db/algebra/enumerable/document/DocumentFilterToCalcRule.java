/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable.document;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.document.DocumentProjectToCalcRule.NearDetector;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.document.DocumentUtil;

public class DocumentFilterToCalcRule extends ConverterRule {

    public static final DocumentFilterToCalcRule INSTANCE = new DocumentFilterToCalcRule();


    public DocumentFilterToCalcRule() {
        super( DocumentFilter.class, r -> true, Convention.NONE, EnumerableConvention.INSTANCE, AlgFactories.LOGICAL_BUILDER, "DocumentToCalcRule_LogicalDocumentFilter" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalDocumentFilter filter = (LogicalDocumentFilter) alg;

        NearDetector nearDetector = new NearDetector();
        filter.accept( nearDetector );
        if ( nearDetector.containsNear ) {
            return null;
        }

        final AlgNode input = filter.getInput();

        // Create a program containing a filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final AlgDataType inputRowType = input.getTupleType();
        final RexProgramBuilder programBuilder = new RexProgramBuilder( inputRowType, rexBuilder );
        NameRefReplacer replacer = new NameRefReplacer( filter.getCluster(), false, alg.getInput( 0 ) );
        programBuilder.addIdentity();
        programBuilder.addCondition( filter.condition.accept( replacer ) );
        final RexProgram program = programBuilder.getProgram();

        return EnumerableCalc.create( convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) ), program );
    }


    /**
     * Visitor which replaces {@link RexNameRef} with {@link RexCall} using {@link OperatorName#MQL_QUERY_VALUE}.
     */
    public static class NameRefReplacer extends RexShuttle {

        private final AlgCluster cluster;
        private final AlgNode input;
        boolean inplace;


        public NameRefReplacer( AlgCluster cluster, boolean inplace, AlgNode input ) {
            this.cluster = cluster;
            this.inplace = inplace;
            this.input = input;
        }


        @Override
        public RexNode visitNameRef( RexNameRef nameRef ) {
            int index = 0;
            if ( input.getModel() == DataModel.RELATIONAL ) {
                // within document model we just access the main field, if already mapped we use the data field
                index = input.getTupleType().getFields().stream().filter( f -> f.getName().equals( "_data" ) ).map( AlgDataTypeField::getIndex ).findAny().orElse( 0 );
            }

            return new RexCall(
                    nameRef.getType(),
                    OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.MQL_QUERY_VALUE ),
                    RexIndexRef.of( index, input.getTupleType() ),
                    DocumentUtil.getStringArray( nameRef.names, cluster ) );
        }

    }

}
