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

package org.polypheny.db.cypher.clause;

import java.util.List;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;

public class CypherReturnClause extends CypherClause {

    public CypherReturnClause( ParserPos pos, boolean distinct, List<CypherReturn> returnItems, List<CypherOrderItem> order, ParserPos pos1, CypherNode skip, ParserPos pos2, CypherExpression limit, ParserPos pos3 ) {
        super( pos, null );
    }

}
