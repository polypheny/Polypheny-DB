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

package org.polypheny.db.cypher.scope;

import lombok.Getter;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.ScopeType;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherDbScope extends CypherScope {

    private final CypherSimpleEither<String, CypherParameter> name;
    private final boolean isDefault;
    private final boolean isHome;
    private final ScopeType type;


    public CypherDbScope( ParserPos pos, CypherSimpleEither<String, CypherParameter> name, boolean isDefault, boolean isHome, ScopeType type ) {
        super( pos );
        this.name = name;
        this.isDefault = isDefault;
        this.isHome = isHome;
        this.type = type;
    }

}
