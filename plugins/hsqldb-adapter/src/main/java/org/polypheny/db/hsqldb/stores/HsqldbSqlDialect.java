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

package org.polypheny.db.hsqldb.stores;


import com.google.common.io.CharStreams;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.hsqldb.jdbc.JDBCClobClient;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.sql.language.fun.SqlFloorFunction;
import org.polypheny.db.type.PolyType;


/**
 * A <code>SqlDialect</code> implementation for the Hsqldb database.
 */
@Slf4j
public class HsqldbSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT = new HsqldbSqlDialect(
            EMPTY_CONTEXT
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "\"" ) );
    public static final int SUBSTITUTION_LENGTH = 20000;


    /**
     * Creates an HsqldbSqlDialect.
     */
    public HsqldbSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public boolean supportsWindowFunctions() {
        return false;
    }


    @Override
    public IntervalParameterStrategy getIntervalParameterStrategy() {
        return IntervalParameterStrategy.NONE;
    }


    @Override
    public boolean supportsJsonFunctions() {
        return true;
    }


    @Override
    public boolean supportsJsonFunction( RexCall call ) {
        OperatorName operatorName = call.getOperator().getOperatorName();
        if ( operatorName == null ) {
            return false;
        }

        return switch ( operatorName ) {
            case JSON_VALUE_EXPRESSION, JSON_STRUCTURED_VALUE_EXPRESSION -> call.operands.size() == 1;
            case JSON_API_COMMON_SYNTAX -> call.operands.size() == 2;
            case JSON_VALUE_ANY -> supportsJsonValueAny( call );
            case JSON_EXISTS -> supportsJsonExists( call );
            default -> false;
        };
    }


    @Override
    public SqlNode getCastSpec( AlgDataType type ) {
        String castSpec;
        switch ( type.getPolyType() ) {
            case ARRAY:
            case DOCUMENT:
                // We need to flag the type with an underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_LONGVARCHAR";
                break;
            case TEXT:
                castSpec = "_VARCHAR(" + SUBSTITUTION_LENGTH + ")";
                break;
            case VARCHAR:
            case VARBINARY:
                if ( type.getPrecision() == -1 ) {
                    castSpec = "_" + type.getPolyType().getName() + "(" + SUBSTITUTION_LENGTH + ")";
                    break;
                } else {
                    return super.getCastSpec( type );
                }
            case FILE:
            case IMAGE:
            case VIDEO:
            case AUDIO:
                // We need to flag the type with an underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_BLOB";
                break;
            case INTERVAL:
                castSpec = "_INTERVAL";
                break;
            default:
                return super.getCastSpec( type );
        }

        return new SqlDataTypeSpec( new SqlIdentifier( castSpec, ParserPos.ZERO ), -1, -1, null, null, ParserPos.ZERO );
    }


    @Override
    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        OperatorName operatorName = call.getOperator().getOperatorName();
        if ( operatorName == OperatorName.JSON_VALUE_ANY && unparseJsonValueAny( writer, call, leftPrec, rightPrec ) ) {
            return;
        }
        if ( operatorName == OperatorName.JSON_EXISTS && unparseJsonExists( writer, call, leftPrec, rightPrec ) ) {
            return;
        }
        if ( operatorName == OperatorName.JSON_API_COMMON_SYNTAX && unparseJsonApiCommonSyntax( writer, call, leftPrec, rightPrec ) ) {
            return;
        }
        if ( isJsonValueExpression( call ) ) {
            unparseJsonDocumentArgument( writer, call, leftPrec, rightPrec );
            return;
        }

        if ( Objects.requireNonNull( call.getKind() ) == Kind.FLOOR ) {
            if ( call.operandCount() != 2 ) {
                super.unparseCall( writer, call, leftPrec, rightPrec );
                return;
            }

            final SqlLiteral timeUnitNode = call.operand( 1 );
            final TimeUnitRange timeUnit = timeUnitNode.value.asSymbol().asEnum( TimeUnitRange.class );

            final String translatedLit = convertTimeUnit( timeUnit );
            SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand( call, translatedLit, timeUnitNode.getPos() );
            SqlFloorFunction.unparseDatetimeFunction( writer, call2, "TRUNC", true );
        } else {
            super.unparseCall( writer, call, leftPrec, rightPrec );
        }
    }


    @Override
    public Expression handleRetrieval( AlgDataType fieldType, Expression child, ParameterExpression resultSet_, int index ) {
        return super.handleRetrieval( fieldType, child, resultSet_, index );
    }


    @SuppressWarnings("unused")
    public static String toString( JDBCClobClient client ) {
        if ( client == null ) {
            return null;
        }
        try {
            return CharStreams.toString( client.getCharacterStream() );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public Optional<String> handleMissingLength( PolyType type ) {
        return switch ( type ) {
            case VARCHAR, VARBINARY, BINARY -> Optional.of( "(" + SUBSTITUTION_LENGTH + ")" );
            default -> Optional.empty();
        };
    }


    @Override
    public SqlNode rewriteSingleValueExpr( SqlNode aggCall ) {
        final SqlNode operand = ((SqlBasicCall) aggCall).operand( 0 );
        final SqlLiteral nullLiteral = SqlLiteral.createNull( ParserPos.ZERO );
        final SqlNode unionOperand = (SqlNode) OperatorRegistry.get( OperatorName.VALUES ).createCall( ParserPos.ZERO, SqlLiteral.createApproxNumeric( "0", ParserPos.ZERO ) );
        // For hsqldb, generate
        //   CASE COUNT(*)
        //   WHEN 0 THEN NULL
        //   WHEN 1 THEN MIN(<result>)
        //   ELSE (VALUES 1 UNION ALL VALUES 1)
        //   END
        final SqlNode caseExpr =
                new SqlCase(
                        ParserPos.ZERO,
                        (SqlNode) OperatorRegistry.get( OperatorName.COUNT ).createCall( ParserPos.ZERO, operand ),
                        SqlNodeList.of(
                                SqlLiteral.createExactNumeric( "0", ParserPos.ZERO ),
                                SqlLiteral.createExactNumeric( "1", ParserPos.ZERO ) ),
                        SqlNodeList.of(
                                nullLiteral,
                                (SqlNode) OperatorRegistry.get( OperatorName.MIN ).createCall( ParserPos.ZERO, operand ) ),
                        (SqlNode) OperatorRegistry.get( OperatorName.SCALAR_QUERY ).createCall(
                                ParserPos.ZERO,
                                OperatorRegistry.get( OperatorName.UNION_ALL ).createCall( ParserPos.ZERO, unionOperand, unionOperand ) ) );

        log.debug( "SINGLE_VALUE rewritten into [{}]", caseExpr );

        return caseExpr;
    }


    private boolean supportsJsonValueAny( RexCall call ) {
        return call.operands.size() == 5
                && isJsonValueBehavior( call.operands.get( 1 ), JsonValueEmptyOrErrorBehavior.NULL )
                && RexLiteral.isNullLiteral( call.operands.get( 2 ) )
                && isJsonValueBehavior( call.operands.get( 3 ), JsonValueEmptyOrErrorBehavior.NULL )
                && RexLiteral.isNullLiteral( call.operands.get( 4 ) );
    }


    private boolean supportsJsonExists( RexCall call ) {
        return call.operands.size() == 1
                || (call.operands.size() == 2 && isJsonExistsBehavior( call.operands.get( 1 ), JsonExistsErrorBehavior.FALSE ));
    }


    private boolean isJsonValueBehavior( RexNode node, JsonValueEmptyOrErrorBehavior behavior ) {
        return node instanceof RexLiteral literal
                && literal.value != null
                && literal.value.isSymbol()
                && literal.value.asSymbol().value == behavior;
    }


    private boolean isJsonExistsBehavior( RexNode node, JsonExistsErrorBehavior behavior ) {
        return node instanceof RexLiteral literal
                && literal.value != null
                && literal.value.isSymbol()
                && literal.value.asSymbol().value == behavior;
    }


    private boolean unparseJsonValueAny( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.operandCount() != 5
                || !isJsonValueBehavior( (SqlNode) call.operand( 1 ), JsonValueEmptyOrErrorBehavior.NULL )
                || !isNullLiteral( (SqlNode) call.operand( 2 ) )
                || !isJsonValueBehavior( (SqlNode) call.operand( 3 ), JsonValueEmptyOrErrorBehavior.NULL )
                || !isNullLiteral( (SqlNode) call.operand( 4 ) ) ) {
            return false;
        }

        SqlCall commonSyntax = asJsonApiCommonSyntax( (SqlNode) call.operand( 0 ) );
        if ( commonSyntax == null ) {
            return false;
        }

        unparseJsonRoutineCall( writer, "POLYPHENY_JSON_VALUE", commonSyntax, leftPrec, rightPrec );
        return true;
    }


    private boolean unparseJsonExists( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.operandCount() != 1
                && !(call.operandCount() == 2 && isJsonExistsBehavior( (SqlNode) call.operand( 1 ), JsonExistsErrorBehavior.FALSE )) ) {
            return false;
        }

        SqlCall commonSyntax = asJsonApiCommonSyntax( (SqlNode) call.operand( 0 ) );
        if ( commonSyntax == null ) {
            return false;
        }

        unparseJsonRoutineCall( writer, "POLYPHENY_JSON_EXISTS", commonSyntax, leftPrec, rightPrec );
        return true;
    }


    private boolean unparseJsonApiCommonSyntax( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.operandCount() != 2 ) {
            return false;
        }

        unparseJsonRoutineCall( writer, "POLYPHENY_JSON_QUERY", call, leftPrec, rightPrec );
        return true;
    }


    private void unparseJsonRoutineCall( SqlWriter writer, String functionName, SqlCall commonSyntax, int leftPrec, int rightPrec ) {
        writer.print( functionName );
        SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")" );
        unparseJsonDocumentArgument( writer, commonSyntax.operand( 0 ), leftPrec, rightPrec );
        writer.sep( "," );
        ((SqlNode) commonSyntax.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.endList( frame );
    }


    private void unparseJsonDocumentArgument( SqlWriter writer, SqlNode node, int leftPrec, int rightPrec ) {
        SqlNode unwrapped = unwrapJsonDocumentExpression( node );
        unwrapped.unparse( writer, leftPrec, rightPrec );
    }


    private SqlNode unwrapJsonDocumentExpression( SqlNode node ) {
        SqlNode unwrapped = node;
        if ( unwrapped instanceof SqlCall call && isJsonValueExpression( call ) && call.operandCount() == 1 ) {
            unwrapped = call.operand( 0 );
        }
        if ( unwrapped instanceof SqlCall call
                && call.getKind() == Kind.CAST
                && call.operandCount() == 2
                && call.operand( 1 ) instanceof SqlDataTypeSpec dataTypeSpec
                && (dataTypeSpec.getType() == PolyType.CHAR || dataTypeSpec.getType() == PolyType.VARCHAR || dataTypeSpec.getType() == PolyType.TEXT) ) {
            unwrapped = call.operand( 0 );
        }
        return unwrapped;
    }


    private boolean isJsonValueExpression( SqlCall call ) {
        OperatorName operatorName = call.getOperator().getOperatorName();
        return operatorName == OperatorName.JSON_VALUE_EXPRESSION
                || operatorName == OperatorName.JSON_STRUCTURED_VALUE_EXPRESSION;
    }


    private SqlCall asJsonApiCommonSyntax( SqlNode node ) {
        if ( node instanceof SqlCall call && call.getOperator().getOperatorName() == OperatorName.JSON_API_COMMON_SYNTAX ) {
            return call;
        }
        return null;
    }


    private boolean isJsonValueBehavior( SqlNode node, JsonValueEmptyOrErrorBehavior behavior ) {
        return node instanceof SqlLiteral literal
                && literal.value != null
                && literal.value.isSymbol()
                && literal.value.asSymbol().value == behavior;
    }


    private boolean isJsonExistsBehavior( SqlNode node, JsonExistsErrorBehavior behavior ) {
        return node instanceof SqlLiteral literal
                && literal.value != null
                && literal.value.isSymbol()
                && literal.value.asSymbol().value == behavior;
    }


    private boolean isNullLiteral( SqlNode node ) {
        return node instanceof SqlLiteral literal && literal.value == null;
    }


    private static String convertTimeUnit( TimeUnitRange unit ) {
        return switch ( unit ) {
            case YEAR -> "YYYY";
            case MONTH -> "MM";
            case DAY -> "DD";
            case WEEK -> "WW";
            case HOUR -> "HH24";
            case MINUTE -> "MI";
            case SECOND -> "SS";
            default -> throw new AssertionError( "could not convert time unit to HSQLDB equivalent: " + unit );
        };
    }


    @Override
    public String timeUnitName( TimeUnitRange unit ) {
        return switch ( unit ) {
            case YEAR -> "YEAR";
            case MONTH -> "MONTH";
            case DAY -> "DAY";
            case HOUR -> "HOUR";
            case MINUTE -> "MINUTE";
            case SECOND -> "SECOND";
            case WEEK -> "WEEK_OF_YEAR";
            case DOW -> "DAY_OF_WEEK";
            case QUARTER -> "QUARTER";
            case DOY -> "DAY_OF_YEAR";
            default -> throw new AssertionError( "could not convert time unit to HSQLDB equivalent: " + unit );
        };
    }

}
