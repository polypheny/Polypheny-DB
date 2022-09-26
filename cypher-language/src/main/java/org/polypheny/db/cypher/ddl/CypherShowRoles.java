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

package org.polypheny.db.cypher.ddl;

import lombok.Getter;
import org.polypheny.db.cypher.CypherYield;
import org.polypheny.db.cypher.admin.CypherWithGraph;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherShowRoles extends CypherWithGraph {

    private final boolean withUsers;
    private final boolean showAll;
    private final CypherYield yield;
    private final CypherReturnClause returnClause;
    private final CypherWhere where;


    public CypherShowRoles(
            ParserPos pos,
            boolean withUsers,
            boolean showAll,
            CypherYield yield,
            CypherReturnClause returnClause,
            CypherWhere where ) {
        super( pos );
        this.withUsers = withUsers;
        this.showAll = showAll;
        this.yield = yield;
        this.returnClause = returnClause;
        this.where = where;
    }

}
