/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.function.Supplier;
import org.polypheny.db.core.Literal;
import org.polypheny.db.core.Node;
import org.polypheny.db.languages.rex.RexSqlStandardConvertletTable;
import org.polypheny.db.languages.rex.RexToSqlNodeConverter;
import org.polypheny.db.languages.rex.RexToSqlNodeConverterImpl;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyphenyDbContextException;

public class SqlRelOptUtil {

    /**
     * Ensures that a source value does not violate the constraint of the target column.
     *
     * @param sourceValue The insert value being validated
     * @param targetConstraint The constraint applied to sourceValue for validation
     * @param errorSupplier The function to apply when validation fails
     */
    public static void validateValueAgainstConstraint( Node sourceValue, RexNode targetConstraint, Supplier<PolyphenyDbContextException> errorSupplier ) {
        if ( !(sourceValue instanceof Literal) ) {
            // We cannot guarantee that the value satisfies the constraint.
            throw errorSupplier.get();
        }
        final Literal insertValue = (Literal) sourceValue;
        final RexLiteral columnConstraint = (RexLiteral) targetConstraint;

        final RexSqlStandardConvertletTable convertletTable = new RexSqlStandardConvertletTable();
        final RexToSqlNodeConverter sqlNodeToRexConverter = new RexToSqlNodeConverterImpl( convertletTable );
        final SqlLiteral constraintValue = (SqlLiteral) sqlNodeToRexConverter.convertLiteral( columnConstraint );

        if ( !insertValue.equals( constraintValue ) ) {
            // The value does not satisfy the constraint.
            throw errorSupplier.get();
        }
    }

}
