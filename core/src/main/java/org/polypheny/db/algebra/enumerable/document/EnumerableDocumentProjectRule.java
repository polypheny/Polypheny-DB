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

package org.polypheny.db.algebra.enumerable.document;

import java.util.function.Predicate;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;

public class EnumerableDocumentProjectRule extends ConverterRule {

    public EnumerableDocumentProjectRule( AlgBuilderFactory algBuilderFactory ) {
        super( LogicalDocumentProject.class, (Predicate<AlgNode>) r -> true, Convention.NONE, EnumerableConvention.INSTANCE, algBuilderFactory, "EnumerableDocumentProjectRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        DocumentProject project = (DocumentProject) alg;
        return new EnumerableDocumentProject(
                project.getCluster(),
                project.getTraitSet().replace( EnumerableConvention.INSTANCE ),
                project.getInput(),
                project.projects,
                project.getRowType() );
    }

}
