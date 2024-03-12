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

package org.polypheny.db.algebra.enumerable.document;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.polypheny.db.algebra.enumerable.document.DocumentFilterToCalcRule.NameRefReplacer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;

public class DocumentSortToSortRule extends AlgOptRule {

    public static final DocumentSortToSortRule INSTANCE = new DocumentSortToSortRule();


    private DocumentSortToSortRule() {
        super( operand( LogicalDocumentSort.class, any() ), DocumentSortToSortRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalDocumentSort sort = call.alg( 0 );
        AlgBuilder builder = call.builder();
        builder.push( sort.getInput() );

        if ( !sort.fieldExps.isEmpty() ) {
            // we have to project the target keys out to use it in the sort
            builder.transform( ModelTrait.RELATIONAL, DocumentType.ofRelational(), false, null );
            NameRefReplacer visitor = new NameRefReplacer( sort.getCluster(), false );
            List<RexNode> inputs = Stream.concat( Stream.of( RexIndexRef.of( 0, DocumentType.ofId().asRelational().getFields() ), RexIndexRef.of( 1, DocumentType.ofId().asRelational().getFields() ) ), sort.fieldExps.stream().map( f -> f.accept( visitor ) ) ).toList();
            builder.push( LogicalRelProject.create( builder.build(), inputs, IntStream.range( 0, inputs.size() ).mapToObj( i -> "in" + i ).toList() ) );
        }
        builder.push( LogicalRelSort.create( builder.build(), sort.fieldExps, sort.collation, sort.offset, sort.fetch ) );

        if ( !sort.fieldExps.isEmpty() ) {
            // we have to restore the initial structure
            builder.push( LogicalRelProject.create( builder.build(),
                    List.of( RexIndexRef.of( 0, DocumentType.ofRelational().getFields() ), RexIndexRef.of( 1, DocumentType.ofRelational().getFields() ) ),
                    List.of( DocumentType.DOCUMENT_ID, DocumentType.DOCUMENT_DATA ) ) );
            builder.transform( ModelTrait.DOCUMENT, DocumentType.ofId(), false, null );
        }

        call.transformTo( builder.build() );
    }

}
