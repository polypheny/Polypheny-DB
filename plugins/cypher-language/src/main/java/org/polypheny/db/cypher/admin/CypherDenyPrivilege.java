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

package org.polypheny.db.cypher.admin;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherPrivilegeType;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.UnsupportedExecutableStatement;


@Getter
public class CypherDenyPrivilege extends CypherAdminCommand implements UnsupportedExecutableStatement {

    private final List<CypherSimpleEither<String, CypherParameter>> roles;
    private final CypherPrivilegeType privilege;


    public CypherDenyPrivilege(
            ParserPos pos,
            List<CypherSimpleEither<String, CypherParameter>> roles,
            CypherPrivilegeType privilege ) {
        super( pos );
        this.roles = roles;
        this.privilege = privilege;
    }

}
