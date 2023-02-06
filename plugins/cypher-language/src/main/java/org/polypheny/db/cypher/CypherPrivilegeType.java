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

package org.polypheny.db.cypher;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.admin.CypherAdminAction;
import org.polypheny.db.cypher.scope.CypherScope;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherPrivilegeType extends CypherNode {

    private final List<CypherAdminAction> privilegeActions;
    private final List<? extends CypherScope> graphScopes;
    private final CypherResource resource;
    private final List<CypherPrivilegeQualifier> qualifiers;


    protected CypherPrivilegeType( ParserPos pos, List<CypherAdminAction> privilegeActions, List<? extends CypherScope> graphScopes, CypherResource resource, List<CypherPrivilegeQualifier> qualifiers ) {
        super( pos );
        this.privilegeActions = privilegeActions;
        this.graphScopes = graphScopes;
        this.resource = resource;
        this.qualifiers = qualifiers;
    }


    public CypherPrivilegeType( ParserPos pos, CypherAdminAction action, List<CypherPrivilegeQualifier> qualifiers ) {
        this( pos, ImmutableList.of( action ), null, null, qualifiers );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PRIVILEGE;
    }

}
