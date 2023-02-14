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

package org.polypheny.db.cypher.mapprojection;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherMapProjection extends CypherExpression {

    private final CypherVariable variable;
    private final List<CypherMPItem> items;


    public CypherMapProjection( ParserPos pos, CypherVariable variable, List<CypherMPItem> items ) {
        super( pos );
        this.variable = variable;
        this.items = items;
    }

}
