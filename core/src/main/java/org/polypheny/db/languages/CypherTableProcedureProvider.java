/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.languages;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Classpath extension for read-only Cypher procedures backed by a relation.
 * Input columns precede output columns in the relation. Invocation selects the rows whose inputs match its arguments.
 */
public interface CypherTableProcedureProvider {

    Optional<Table> find( String graphNamespace, List<String> qualifiedName );


    /**
     * DECIMAL represents Cypher NUMBER, which accepts integers and floating-point values.
     */
    record Field(String name, PolyType type, boolean nullable) {

        public Field {
            Objects.requireNonNull( name );
            Objects.requireNonNull( type );
        }

        public static boolean accepts( PolyType expected, boolean nullable, PolyType actual ) {
            if ( actual == PolyType.NULL ) {
                return nullable;
            }
            return switch ( expected ) {
                case DECIMAL, DOUBLE, FLOAT, REAL -> PolyType.NUMERIC_TYPES.contains( actual );
                case BIGINT -> PolyType.INT_TYPES.contains( actual );
                case VARCHAR -> actual == PolyType.VARCHAR || actual == PolyType.CHAR || actual == PolyType.TEXT;
                case ANY -> true;
                default -> expected == actual;
            };
        }

    }


    /**
     * A void (zero-output) procedure returns no records to its caller. In a composed query, it preserves the input rows.
     */
    record Table(List<Field> inputs, List<Field> outputs, List<List<PolyValue>> rows) {

        public Table {
            inputs = List.copyOf( inputs );
            outputs = List.copyOf( outputs );
            rows = rows.stream().map( List::copyOf ).toList();
            int width = inputs.size() + outputs.size();
            if ( rows.stream().anyMatch( row -> row.size() != width ) ) {
                throw new IllegalArgumentException( "Procedure relation width does not match its signature." );
            }
        }

    }

}
