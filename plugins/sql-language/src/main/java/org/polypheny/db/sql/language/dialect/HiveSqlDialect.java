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

package org.polypheny.db.sql.language.dialect;


import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlWriter;


/**
 * A <code>SqlDialect</code> implementation for the Apache Hive database.
 */
public class HiveSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
            new HiveSqlDialect( EMPTY_CONTEXT
                    .withNullCollation( NullCollation.LOW ) );

    private final boolean emulateNullDirection;


    /**
     * Creates a HiveSqlDialect.
     */
    public HiveSqlDialect( Context context ) {
        super( context );
        // Since 2.1.0, Hive natively supports "NULLS FIRST" and "NULLS LAST".
        // See https://issues.apache.org/jira/browse/HIVE-12994.
        emulateNullDirection = true;
    }


    @Override
    protected boolean allowsAs() {
        return false;
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public SqlNode emulateNullDirection( SqlNode node, boolean nullsFirst, boolean desc ) {
        if ( emulateNullDirection ) {
            return emulateNullDirectionWithIsNull( node, nullsFirst, desc );
        }

        return null;
    }


    @Override
    public void unparseCall( final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec ) {
        switch ( call.getKind() ) {
            case POSITION:
                final SqlWriter.Frame frame = writer.startFunCall( "INSTR" );
                writer.sep( "," );
                ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
                writer.sep( "," );
                ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
                if ( 3 == call.operandCount() ) {
                    throw new GenericRuntimeException( "3rd operand Not Supported for Function INSTR in Hive" );
                }
                writer.endFunCall( frame );
                break;
            case MOD:
                SqlOperator op = OperatorRegistry.get( OperatorName.PERCENT_REMAINDER, SqlOperator.class );
                SqlSyntax.BINARY.unparse( writer, op, call, leftPrec, rightPrec );
                break;
            default:
                super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }

}
