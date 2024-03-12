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

package org.polypheny.db.cypher.clause;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.ConstraintType;
import org.polypheny.db.cypher.ConstraintVersion;
import org.polypheny.db.cypher.CypherSimpleEither;
import org.polypheny.db.cypher.ddl.CypherSchemaCommand;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


@Getter
public class CypherCreateConstraint extends CypherSchemaCommand implements ExecutableStatement {

    private final ConstraintType constraintType;
    private final boolean replace;
    private final boolean ifNotExists;
    private final String name;
    private final CypherVariable variable;
    private final StringPos parserPosStringPos;
    private final List<CypherProperty> properties;
    private final CypherSimpleEither<?, ?> options;
    private final boolean containsOn;
    private final ConstraintVersion constraintVersion;


    public CypherCreateConstraint(
            ParserPos pos,
            ConstraintType constraintType,
            boolean replace,
            boolean ifNotExists,
            String name,
            CypherVariable variable,
            StringPos parserPosStringPos,
            List<CypherProperty> properties,
            CypherSimpleEither<?, ?> options,
            boolean containsOn,
            ConstraintVersion constraintVersion ) {
        super( pos );
        this.constraintType = constraintType;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.name = name;
        this.variable = variable;
        this.parserPosStringPos = parserPosStringPos;
        this.properties = properties;
        this.options = options;
        this.containsOn = containsOn;
        this.constraintVersion = constraintVersion;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        throw new GenericRuntimeException( "Constraints are not supported yet for graph data." );
    }

}
