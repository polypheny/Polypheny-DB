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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSetOperator;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlWriter;


/**
 * A <code>SqlDialect</code> implementation for Google BigQuery's "Standard SQL" dialect.
 */
public class BigQuerySqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT =
            new BigQuerySqlDialect(
                    EMPTY_CONTEXT
                            .withNullCollation( NullCollation.LOW )
                            .withIdentifierQuoteString( "`" ) );


    /**
     * Creates a BigQuerySqlDialect.
     */
    public BigQuerySqlDialect( SqlDialect.Context context ) {
        super( context );
    }


    @Override
    public void unparseCall( final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec ) {
        switch ( call.getKind() ) {
            case POSITION:
                final SqlWriter.Frame frame = writer.startFunCall( "STRPOS" );
                writer.sep( "," );
                ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
                writer.sep( "," );
                ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
                if ( 3 == call.operandCount() ) {
                    throw new GenericRuntimeException( "3rd operand Not Supported for Function STRPOS in Big Query" );
                }
                writer.endFunCall( frame );
                break;
            case UNION:
                if ( !((SqlSetOperator) call.getOperator()).isAll() ) {
                    SqlSyntax.BINARY.unparse( writer, UNION_DISTINCT, call, leftPrec, rightPrec );
                }
                break;
            case EXCEPT:
                if ( !((SqlSetOperator) call.getOperator()).isAll() ) {
                    SqlSyntax.BINARY.unparse( writer, EXCEPT_DISTINCT, call, leftPrec, rightPrec );
                }
                break;
            case INTERSECT:
                if ( !((SqlSetOperator) call.getOperator()).isAll() ) {
                    SqlSyntax.BINARY.unparse( writer, INTERSECT_DISTINCT, call, leftPrec, rightPrec );
                }
                break;
            default:
                super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }


    /**
     * List of BigQuery Specific Operators needed to form Syntactically Correct SQL.
     */
    private static final SqlOperator UNION_DISTINCT = new SqlSetOperator( "UNION DISTINCT", Kind.UNION, 14, false );

    private static final SqlSetOperator EXCEPT_DISTINCT = new SqlSetOperator( "EXCEPT DISTINCT", Kind.EXCEPT, 14, false );

    private static final SqlSetOperator INTERSECT_DISTINCT = new SqlSetOperator( "INTERSECT DISTINCT", Kind.INTERSECT, 18, false );

}

