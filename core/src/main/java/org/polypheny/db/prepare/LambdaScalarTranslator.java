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

package org.polypheny.db.prepare;

import java.util.List;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;

/**
 * Translator that looks for parameters.
 */
class LambdaScalarTranslator extends EmptyScalarTranslator {

    private final List<ParameterExpression> parameterList;
    private final List<RexNode> values;


    LambdaScalarTranslator( RexBuilder rexBuilder, List<ParameterExpression> parameterList, List<RexNode> values ) {
        super( rexBuilder );
        this.parameterList = parameterList;
        this.values = values;
    }


    @Override
    public RexNode parameter( ParameterExpression param ) {
        int i = parameterList.indexOf( param );
        if ( i >= 0 ) {
            return values.get( i );
        }
        throw new GenericRuntimeException( "unknown parameter " + param );
    }

}
