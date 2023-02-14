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

package org.polypheny.db.cypher;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherOrderItem;
import org.polypheny.db.cypher.clause.CypherReturn;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherYield extends CypherClause {

    private final boolean returnAll;
    private final List<CypherReturn> returnItems;
    private final ParserPos nextPos;
    private final List<CypherOrderItem> orders;
    private final ParserPos skipPos;


    protected CypherYield( ParserPos pos, boolean returnAll, List<CypherReturn> returnItems, ParserPos nextPos, List<CypherOrderItem> orders, ParserPos skipPos, CypherExpression skip, ParserPos limitPos, CypherExpression limit, ParserPos wherePos, CypherWhere where ) {
        super( pos );
        this.returnAll = returnAll;
        this.returnItems = returnItems;
        this.nextPos = nextPos;
        this.orders = orders;
        this.skipPos = skipPos;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.YIELD;
    }

}
