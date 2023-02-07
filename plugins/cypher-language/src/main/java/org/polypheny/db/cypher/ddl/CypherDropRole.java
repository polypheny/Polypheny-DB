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

package org.polypheny.db.cypher.ddl;

import lombok.Getter;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.admin.CypherAdminCommand;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherDropRole extends CypherAdminCommand {

    private final CypherSimpleEither<String, CypherParameter> roleName;
    private final boolean ifExists;


    public CypherDropRole( ParserPos pos, CypherSimpleEither<String, CypherParameter> roleName, boolean ifExists ) {
        super( pos );
        this.roleName = roleName;
        this.ifExists = ifExists;
    }

}
