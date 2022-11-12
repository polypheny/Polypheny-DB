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

import java.util.ArrayList;
import lombok.Getter;
import org.polypheny.db.cypher.MergeActionType;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherMerge extends CypherClause {

    private final CypherPattern pattern;
    private final ArrayList<CypherSetClause> clauses;
    private final ArrayList<MergeActionType> actionTypes;
    private final ArrayList<ParserPos> positions;


    public CypherMerge(
            ParserPos pos,
            CypherPattern pattern,
            ArrayList<CypherSetClause> clauses,
            ArrayList<MergeActionType> actionTypes,
            ArrayList<ParserPos> positions ) {
        super( pos );
        this.pattern = pattern;
        this.clauses = clauses;
        this.actionTypes = actionTypes;
        this.positions = positions;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.MERGE;
    }

}
