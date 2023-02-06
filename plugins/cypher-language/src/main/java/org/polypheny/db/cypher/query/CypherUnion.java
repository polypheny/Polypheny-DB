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

import lombok.Getter;
import org.polypheny.db.cypher.clause.CypherQuery;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherUnion extends CypherQuery {

    private final CypherQuery left;
    private final CypherQuery right;
    private final boolean all;


    public CypherUnion( ParserPos pos, CypherQuery left, CypherQuery right, boolean all ) {
        super( pos );
        this.left = left;
        this.right = right;
        this.all = all;
    }


    @Override
    public void accept( CypherVisitor visitor ) {
        visitor.visit( this );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.UNION;
    }

}
