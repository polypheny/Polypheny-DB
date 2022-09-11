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

package org.polypheny.db.cypher.admin;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherPrivilegeType;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.UnsupportedExecutableStatement;


@Getter
public class CypherRevokePrivilege extends CypherAdminCommand implements UnsupportedExecutableStatement {

    private final List<CypherSimpleEither<String, CypherParameter>> roles;
    private final CypherPrivilegeType privilege;
    private final boolean revokeGrant;
    private final boolean revokeDeny;


    public CypherRevokePrivilege(
            ParserPos pos,
            List<CypherSimpleEither<String, CypherParameter>> roles,
            CypherPrivilegeType privilege,
            boolean revokeGrant,
            boolean revokeDeny ) {
        super( pos );
        this.roles = roles;
        this.privilege = privilege;
        this.revokeGrant = revokeGrant;
        this.revokeDeny = revokeDeny;
    }

}
