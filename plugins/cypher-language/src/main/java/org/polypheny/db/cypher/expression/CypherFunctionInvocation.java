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

package org.polypheny.db.cypher.expression;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;

@Getter
public class CypherFunctionInvocation extends CypherExpression {

    private final ParserPos namePos;
    private final List<String> namespace;
    private final boolean distinct;
    private final List<CypherExpression> arguments;

    private static final List<String> operatorNames = Arrays.stream( OperatorName.values() ).map( Enum::name ).toList();
    private final OperatorName op;


    public CypherFunctionInvocation( ParserPos beforePos, ParserPos namePos, List<String> namespace, String image, boolean distinct, List<CypherExpression> arguments ) {
        super( beforePos );
        this.namePos = namePos;
        this.namespace = namespace;
        if ( operatorNames.contains( image.toUpperCase( Locale.ROOT ) ) ) {
            this.op = OperatorName.valueOf( image.toUpperCase( Locale.ROOT ) );
        } else {
            throw new GenericRuntimeException( "Used function is not supported!" );
        }
        this.distinct = distinct;
        this.arguments = arguments;
    }

}
