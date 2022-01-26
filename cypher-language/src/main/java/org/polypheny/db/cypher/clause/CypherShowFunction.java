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

import org.polypheny.db.cypher.ShowCommandFilterType;
import org.polypheny.db.languages.ParserPos;

public class CypherShowFunction extends CypherClause {

    public CypherShowFunction( ParserPos pos, ShowCommandFilterType functionType, boolean currentUser, String user, CypherWhere where, boolean yield ) {
        super( pos );
    }

}
