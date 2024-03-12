/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.List;
import lombok.Getter;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherPrivilegeQualifier extends CypherNode {

    private final List<String> names;
    private final QualifierType type;


    public CypherPrivilegeQualifier( ParserPos pos, List<String> names, QualifierType type ) {
        super( pos );
        this.names = names;
        this.type = type;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PRIVILEGE;
    }


    public enum QualifierType {
        ALL, RELATIONSHIP, LABEL, ALL_ELEMENTS, ELEMENT, USER, ALL_LABELS, ALL_NAMESPACES, ALL_USERS, ALL_RELATIONSHIP
    }

}
