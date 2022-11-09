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

package org.polypheny.db.cypher.admin;

import lombok.Getter;
import org.polypheny.db.cypher.CypherYield;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.UnsupportedExecutableStatement;

@Getter
public class CypherShowAllPrivileges extends CypherWithGraph implements UnsupportedExecutableStatement {

    private final boolean asCommand;
    private final boolean asRevoke;
    private final CypherYield yield;
    private final CypherReturnClause returnClause;
    private final CypherWhere where;


    public CypherShowAllPrivileges(
            ParserPos pos,
            boolean asCommand,
            boolean asRevoke,
            CypherYield yield,
            CypherReturnClause returnClause,
            CypherWhere where ) {
        super( pos );
        this.asCommand = asCommand;
        this.asRevoke = asRevoke;
        this.yield = yield;
        this.returnClause = returnClause;
        this.where = where;
    }

}
