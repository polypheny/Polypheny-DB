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

import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.parser.ParseException;

public class CypherSyntaxException extends RuntimeException {

    public CypherSyntaxException( ParseException e, int beginOffset, int beginLine, int beginColumn ) {
        throw new GenericRuntimeException( e.getMessage() );
    }


    public static String relationshipPatternNotAllowed( ConstraintType constraintType ) {
        return "Used relationship pattern is not allowed: " + constraintType.name() + ".";
    }

}
