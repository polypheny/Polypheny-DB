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

package org.polypheny.db.sql.language.dialect;


import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.fun.SqlFloorFunction;


/**
 * A <code>SqlDialect</code> implementation for the Oracle database.
 */
public class OracleSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
            new OracleSqlDialect( EMPTY_CONTEXT
                    .withDatabaseProduct( DatabaseProduct.ORACLE )
                    .withIdentifierQuoteString( "\"" ) );


    /**
     * Creates an OracleSqlDialect.
     */
    public OracleSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    protected boolean allowsAs() {
        return false;
    }


    @Override
    public boolean supportsAliasedValues() {
        return false;
    }


    @Override
    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.getOperator().getOperatorName() == OperatorName.SUBSTRING ) {
            SqlUtil.unparseFunctionSyntax( OracleSqlOperatorTable.SUBSTR, writer, call );
        } else {
            switch ( call.getKind() ) {
                case FLOOR:
                    if ( call.operandCount() != 2 ) {
                        super.unparseCall( writer, call, leftPrec, rightPrec );
                        return;
                    }

                    final SqlLiteral timeUnitNode = call.operand( 1 );
                    final TimeUnitRange timeUnit = timeUnitNode.getValueAs( TimeUnitRange.class );

                    SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand( call, timeUnit.name(), timeUnitNode.getPos() );
                    SqlFloorFunction.unparseDatetimeFunction( writer, call2, "TRUNC", true );
                    break;

                default:
                    super.unparseCall( writer, call, leftPrec, rightPrec );
            }
        }
    }

}

