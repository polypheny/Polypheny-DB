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
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherCreateUser extends CypherAdminCommand {

    private final boolean replace;
    private final boolean ifNotExists;
    private final CypherSimpleEither<String, CypherParameter> username;
    private final CypherExpression password;
    private final boolean encrypted;
    private final Boolean orElse;
    private final Boolean orElse1;
    private final CypherSimpleEither<String, CypherParameter> orElse2;


    public CypherCreateUser(
            ParserPos pos,
            boolean replace,
            boolean ifNotExists,
            CypherSimpleEither<String, CypherParameter> username,
            CypherExpression password,
            boolean encrypted,
            Boolean orElse,
            Boolean orElse1,
            CypherSimpleEither<String, CypherParameter> orElse2 ) {
        super( pos );
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.username = username;
        this.password = password;
        this.encrypted = encrypted;
        this.orElse = orElse;
        this.orElse1 = orElse1;
        this.orElse2 = orElse2;
    }

}
