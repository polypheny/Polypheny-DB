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

package org.polypheny.db.cypher.expression;

import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherUnary extends CypherExpression {

    private final OperatorName unaryPlus;
    private final CypherExpression expression;


    public CypherUnary( ParserPos pos, OperatorName unaryPlus, CypherExpression expression ) {
        super( pos );
        this.unaryPlus = unaryPlus;
        this.expression = expression;
    }

}
