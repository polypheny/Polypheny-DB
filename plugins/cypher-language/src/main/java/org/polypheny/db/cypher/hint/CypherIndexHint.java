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

package org.polypheny.db.cypher.hint;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.HintIndexType;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherIndexHint extends CypherHint {

    private final CypherVariable variable;
    private final String labelOrRelType;
    private final List<String> propNames;
    private final boolean seek;
    private final HintIndexType indexType;


    public CypherIndexHint( ParserPos pos, CypherVariable variable, String labelOrRelType, List<String> propNames, boolean seek, HintIndexType indexType ) {
        super( pos );
        this.variable = variable;
        this.labelOrRelType = labelOrRelType;
        this.propNames = propNames;
        this.seek = seek;
        this.indexType = indexType;
    }

}
