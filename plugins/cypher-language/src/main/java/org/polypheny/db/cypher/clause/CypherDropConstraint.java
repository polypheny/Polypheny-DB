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

package org.polypheny.db.cypher.clause;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.cypher.ConstraintType;
import org.polypheny.db.cypher.ddl.CypherSchemaCommand;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;


@Getter
public class CypherDropConstraint extends CypherSchemaCommand {

    private String name;
    private boolean ifExists;
    private ConstraintType constraintType;
    private CypherVariable variable;
    private StringPos parserPosStringPos;
    private List<CypherProperty> properties;


    public CypherDropConstraint(
            ParserPos pos,
            ConstraintType constraintType,
            CypherVariable variable,
            StringPos parserPosStringPos,
            List<CypherProperty> properties ) {
        super( pos );
        this.constraintType = constraintType;
        this.variable = variable;
        this.parserPosStringPos = parserPosStringPos;
        this.properties = properties;
    }


    public CypherDropConstraint( ParserPos pos, String name, boolean ifExists ) {
        super( pos );
        this.name = name;
        this.ifExists = ifExists;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.DROP;
    }

}
