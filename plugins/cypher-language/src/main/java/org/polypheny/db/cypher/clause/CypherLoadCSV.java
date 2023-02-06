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

package org.polypheny.db.cypher.clause;

import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherLoadCSV extends CypherClause {

    private final boolean headers;
    private final CypherExpression expression;
    private final CypherVariable variable;
    private final String separator;


    public CypherLoadCSV(
            ParserPos pos,
            boolean headers,
            CypherExpression expression,
            CypherVariable variable,
            String separator ) {
        super( pos );
        this.headers = headers;
        this.expression = expression;
        this.variable = variable;
        this.separator = separator;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.LOAD_CSV;
    }

}
