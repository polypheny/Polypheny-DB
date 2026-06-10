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

package org.polypheny.db.adapter.postgres;


import com.google.common.collect.ImmutableList;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgDataTypeSystemImpl;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.fun.SqlFloorFunction;
import org.polypheny.db.sql.language.validate.SqlType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * A <code>SqlDialect</code> implementation for the PostgreSQL database.
 */
public class PostgresqlSqlDialect extends SqlDialect {

    /**
     * PostgreSQL type system.
     */
    private static final AlgDataTypeSystem POSTGRESQL_TYPE_SYSTEM =
            new AlgDataTypeSystemImpl() {
                @Override
                public int getMaxPrecision( PolyType typeName ) {
                    if ( Objects.requireNonNull( typeName ) == PolyType.VARCHAR ) {// From htup_details.h in postgresql:
                        // MaxAttrSize is a somewhat arbitrary upper limit on the declared size of data fields of char(n) and similar types.  It need not have anything
                        // directly to do with the *actual* upper limit of varlena values, which is currently 1Gb (see TOAST structures in postgres.h).  I've set it
                        // at 10Mb which seems like a reasonable number --- tgl 8/6/00.
                        return 10 * 1024 * 1024;
                    }
                    return super.getMaxPrecision( typeName );
                }
            };

    public static final SqlDialect DEFAULT =
            new PostgresqlSqlDialect( EMPTY_CONTEXT
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "\"" )
                    .withDataTypeSystem( POSTGRESQL_TYPE_SYSTEM ) );


    /**
     * Creates a PostgresqlSqlDialect.
     */
    public PostgresqlSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public boolean supportsNestedArrays() {
        return false;
    }


    @Override
    public boolean supportsArrays() {
        return true;
    }


    @Override
    public List<OperatorName> supportedGeoFunctions() {
        return ImmutableList.of( OperatorName.ST_GEOMFROMTEXT, OperatorName.ST_TRANSFORM, OperatorName.ST_EQUALS,
                OperatorName.ST_ISSIMPLE, OperatorName.ST_ISCLOSED, OperatorName.ST_ISEMPTY, OperatorName.ST_ISRING,
                OperatorName.ST_NUMPOINTS, OperatorName.ST_DIMENSION, OperatorName.ST_LENGTH, OperatorName.ST_AREA,
                OperatorName.ST_ENVELOPE, OperatorName.ST_BOUNDARY, OperatorName.ST_CONVEXHULL, OperatorName.ST_CENTROID,
                OperatorName.ST_CENTROID, OperatorName.ST_DISJOINT, OperatorName.ST_TOUCHES, OperatorName.ST_INTERSECTS,
                OperatorName.ST_CROSSES, OperatorName.ST_WITHIN, OperatorName.ST_CONTAINS, OperatorName.ST_OVERLAPS,
                OperatorName.ST_COVERS, OperatorName.ST_COVEREDBY, OperatorName.ST_RELATE,
                OperatorName.ST_INTERSECTION, OperatorName.ST_UNION, OperatorName.ST_DIFFERENCE, OperatorName.ST_SYMDIFFERENCE,
                OperatorName.ST_X, OperatorName.ST_Y, OperatorName.ST_Z, OperatorName.ST_STARTPOINT, OperatorName.ST_ENDPOINT,
                OperatorName.ST_EXTERIORRING, OperatorName.ST_NUMINTERIORRING, OperatorName.ST_INTERIORRINGN,
                OperatorName.ST_NUMGEOMETRIES, OperatorName.ST_GEOMETRYN );
    }


    @Override
    public boolean supportsGeoJson() {
        return true;
    }


    @Override
    public boolean supportsPostGIS() {
        return true;
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
            case JSON_VALUE_EXPRESSION, JSON_VALUE_EXPRESSION_EXCLUDED, JSON_STRUCTURED_VALUE_EXPRESSION -> call.operands.size() == 1;
            case JSON_API_COMMON_SYNTAX -> call.operands.size() == 2;
            case JSON_VALUE_ANY -> supportsJsonValueAny( call );
            case JSON_EXISTS -> supportsJsonExists( call );
            default -> false;
        };
    }


    @Override
    public void setDocumentDynamicParam( PreparedStatement preparedStatement, int index, PolyValue value ) throws SQLException {
        preparedStatement.setObject( index, value.asDocument().toJson(), Types.OTHER );
    }


    @Override
    public Optional<String> handleMissingLength( PolyType type ) {
        return switch ( type ) {
            case VARBINARY, VARCHAR, BINARY -> Optional.of( "VARYING" );
            default -> Optional.empty();
        };
    }


    @Override
    public Expression handleRetrieval( AlgDataType fieldType, Expression child, ParameterExpression resultSet_, int index ) {
        if ( fieldType.getPolyType() == PolyType.GEOMETRY ) {
            if ( supportsPostGIS() ) {
                // convert postgis geometry (net.postgres.PGgeometry) that is a wrapper of org.postgresql.util.PGobject (has getValue() method to return string) into a string
                return Expressions.call( PolyGeometry.class, fieldType.isNullable() ? "ofNullable" : "of", Expressions.convert_( Expressions.call( Expressions.convert_( child, net.postgis.jdbc.PGgeometry.class ), "getValue" ), String.class ) );
            } else if ( supportsGeoJson() ) {
                return Expressions.call( PolyGeometry.class, fieldType.isNullable() ? "fromNullableGeoJson" : "fromGeoJson", Expressions.convert_( child, String.class ) );
            }
        }
        return super.handleRetrieval( fieldType, child, resultSet_, index );
    }


    @Override
    public SqlNode getCastSpec( AlgDataType type ) {
        String castSpec;
        switch ( type.getPolyType() ) {
            case TINYINT:
                // Postgres has no tinyint (1 byte), so instead cast to smallint (2 bytes)
                castSpec = "_smallint";
                break;
            case DOUBLE:
                // Postgres has a double type but it is named differently
                castSpec = "_double precision";
                break;
            case GEOMETRY:
                castSpec = "_GEOMETRY";
                break;
            case DOCUMENT:
                castSpec = "_JSONB";
                break;
            case VARBINARY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case AUDIO:
                castSpec = "_BYTEA";
                break;
            case ARRAY:
                if ( type.getComponentType().getPolyType() == PolyType.ARRAY ) {
                    castSpec = "_TEXT";
                    break;
                }

                AlgDataType tt = type;
                StringBuilder brackets = new StringBuilder( "[]" );
                while ( tt.getComponentType().getPolyType() == PolyType.ARRAY ) {
                    tt = tt.getComponentType();
                    brackets.append( "[]" );
                }
                PolyType t = tt.getComponentType().getPolyType();
                castSpec = switch ( t ) {
                    case TINYINT -> "_smallint" + brackets;
                    case DOUBLE -> "_double precision" + brackets;
                    default -> "_" + t.getName() + brackets;
                };
                break;
            case INTERVAL:
                castSpec = "interval";
                break;
            default:
                return super.getCastSpec( type );
        }

        return new SqlDataTypeSpec( new SqlIdentifier( castSpec, ParserPos.ZERO ), -1, -1, null, null, ParserPos.ZERO );
    }


    @Override
    public String getArrayComponentTypeString( SqlType type ) {
        return switch ( type ) {
            case TINYINT -> "int2"; // Postgres has no tinyint (1 byte), so instead cast to smallint (2 bytes)
            case DOUBLE -> "float8";
            case REAL -> "float4";
            default -> super.getArrayComponentTypeString( type );
        };
    }


    @Override
    public boolean supportsNestedAggregations() {
        return false;
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
            unparseJsonDocumentExpression( writer, call, leftPrec, rightPrec );
            return;
        }

        switch ( call.getKind() ) {
            case FLOOR:
                if ( call.operandCount() != 2 ) {
                    super.unparseCall( writer, call, leftPrec, rightPrec );
                    return;
                }

                final SqlLiteral timeUnitNode = call.operand( 1 );
                final TimeUnitRange timeUnit = timeUnitNode.value.asSymbol().asEnum( TimeUnitRange.class );

                SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand( call, timeUnit.name(), timeUnitNode.getPos() );
                SqlFloorFunction.unparseDatetimeFunction( writer, call2, "DATE_TRUNC", false );
                break;

            case EXTRACT:
                if ( call.getOperandList().get( 0 ) instanceof SqlLiteral && ((SqlLiteral) call.getOperandList().get( 0 )).value.asSymbol().value instanceof TimeUnitRange ) {
                    TimeUnitRange unitRange = ((SqlLiteral) call.getOperandList().get( 0 )).value.asSymbol().asEnum( TimeUnitRange.class );
                    if ( unitRange == TimeUnitRange.DOW ) {
                        SqlFunction func = new SqlFunction(
                                "DOW_SUNDAY",
                                Kind.OTHER_FUNCTION,
                                ReturnTypes.INTEGER,
                                null,
                                null,
                                FunctionCategory.USER_DEFINED_FUNCTION );
                        SqlCall call1 = (SqlCall) call.getOperator().createCall( call.getPos(), call.getOperandList().get( 1 ) );
                        SqlUtil.unparseFunctionSyntax( func, writer, call1 );
                    } else {
                        super.unparseCall( writer, call, leftPrec, rightPrec );
                    }
                } else {
                    super.unparseCall( writer, call, leftPrec, rightPrec );
                }
                break;
            case MIN:
            case MAX:
                // min( boolean ) should stay boolean and return true if one value is true else false, this is not the case in postgres
                SqlBasicCall basicCall = (SqlBasicCall) call;
                if ( basicCall.getOperandList().size() == 1
                        && basicCall.getOperandList().get( 0 ) instanceof SqlBasicCall childCall
                        && childCall.getOperandList().size() == 2
                        && childCall.getOperator().getKind() == Kind.CAST
                        && childCall.getOperandList().get( 1 ) instanceof SqlDataTypeSpec dataTypeSpec
                        && dataTypeSpec.getType() == PolyType.BOOLEAN ) {
                    writer.print( call.getKind() == Kind.MIN ? "bool_or(" : "bool_and(" );
                    childCall.unparse( writer, leftPrec, rightPrec );
                    writer.print( ")" );
                    return;
                } else {
                    super.unparseCall( writer, call, leftPrec, rightPrec );
                }
                break;

            default:
                super.unparseCall( writer, call, leftPrec, rightPrec );
        }
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

        writer.print( "(CASE WHEN jsonb_typeof(" );
        unparseJsonPathQueryFirst( writer, commonSyntax, leftPrec, rightPrec );
        writer.print( ") = 'object' THEN NULL WHEN jsonb_typeof(" );
        unparseJsonPathQueryFirst( writer, commonSyntax, leftPrec, rightPrec );
        writer.print( ") = 'array' AND EXISTS (SELECT 1 FROM jsonb_array_elements(" );
        unparseJsonPathQueryFirst( writer, commonSyntax, leftPrec, rightPrec );
        writer.print( ") AS elem(value) WHERE jsonb_typeof(elem.value) IN ('object', 'array')) THEN NULL WHEN jsonb_typeof(" );
        unparseJsonPathQueryFirst( writer, commonSyntax, leftPrec, rightPrec );
        writer.print( ") = 'array' THEN " );
        unparseJsonPathQueryFirst( writer, commonSyntax, leftPrec, rightPrec );
        writer.print( "::text ELSE " );
        unparseJsonPathQueryFirst( writer, commonSyntax, leftPrec, rightPrec );
        writer.print( " #>> '{}' END)" );
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

        writer.print( "jsonb_path_exists(" );
        unparseJsonDocumentExpression( writer, commonSyntax.operand( 0 ), leftPrec, rightPrec );
        writer.print( ", " );
        ((SqlNode) commonSyntax.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.print( ", " );
        writer.print( "'{}'::jsonb" );
        writer.print( ", " );
        writer.print( "true" );
        writer.print( ")" );
        return true;
    }


    private boolean unparseJsonApiCommonSyntax( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.operandCount() != 2 ) {
            return false;
        }

        unparseJsonPathQueryFirst( writer, call, leftPrec, rightPrec );
        return true;
    }


    private void unparseJsonPathQueryFirst( SqlWriter writer, SqlCall commonSyntax, int leftPrec, int rightPrec ) {
        writer.print( "jsonb_path_query_first(" );
        unparseJsonDocumentExpression( writer, commonSyntax.operand( 0 ), leftPrec, rightPrec );
        writer.print( ", " );
        ((SqlNode) commonSyntax.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.print( ", " );
        writer.print( "'{}'::jsonb" );
        writer.print( ", " );
        writer.print( "true" );
        writer.print( ")" );
    }


    private void unparseJsonDocumentExpression( SqlWriter writer, SqlNode node, int leftPrec, int rightPrec ) {
        SqlNode unwrapped = unwrapJsonDocumentExpression( node );
        writer.print( "(" );
        unwrapped.unparse( writer, leftPrec, rightPrec );
        writer.print( ")::jsonb" );
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
                && List.of( PolyType.CHAR, PolyType.VARCHAR, PolyType.TEXT ).contains( dataTypeSpec.getType() ) ) {
            unwrapped = call.operand( 0 );
        }
        return unwrapped;
    }


    private boolean isJsonValueExpression( SqlCall call ) {
        OperatorName operatorName = call.getOperator().getOperatorName();
        return operatorName == OperatorName.JSON_VALUE_EXPRESSION
                || operatorName == OperatorName.JSON_VALUE_EXPRESSION_EXCLUDED
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

}
