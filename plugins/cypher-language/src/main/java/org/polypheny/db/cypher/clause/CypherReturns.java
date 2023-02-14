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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherReturns extends CypherClause {

    private final boolean returnAll;
    private final List<CypherReturn> returns;


    public CypherReturns( ParserPos pos, boolean returnAll, List<CypherReturn> returns ) {
        super( pos );
        this.returnAll = returnAll;
        if ( returnAll ) {
            // Attach a star select, which preserves all previous variables
            List<CypherReturn> list = new ArrayList<>( returns );
            list.add( 0, new CypherStarReturn( pos ) );
            this.returns = list;
        } else {
            this.returns = returns;
        }
    }


    public void add( CypherReturn cReturn ) {
        this.returns.add( cReturn );
    }


    public List<CypherReturn> getReturns() {
        return returns;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.RETURN;
    }

}
