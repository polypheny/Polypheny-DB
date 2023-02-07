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

package org.polypheny.db.cypher.query;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherQuery;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherPeriodicCommit extends CypherQuery {


    private final ParserPos commitPos;
    private final String batchSize;
    private final CypherClause loadCsv;
    private final List<CypherClause> queryBody;


    public CypherPeriodicCommit( ParserPos pos, ParserPos commitPos, String batchSize, CypherClause loadCsv, List<CypherClause> queryBody ) {
        super( pos );
        this.commitPos = commitPos;
        this.batchSize = batchSize;
        this.loadCsv = loadCsv;
        this.queryBody = queryBody;
    }


    @Override
    public void accept( CypherVisitor visitor ) {
        visitor.visit( this );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PERIODIC_COMMIT;
    }

}
