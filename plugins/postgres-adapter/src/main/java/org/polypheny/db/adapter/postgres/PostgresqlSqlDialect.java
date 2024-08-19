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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgDataTypeSystemImpl;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.TimeUnitRange;
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

}
