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

package org.polypheny.db.adapter.monetdb;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nl.cwi.monetdb.jdbc.MonetClob;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.PolyType;


/**
 * A <code>SqlDialect</code> implementation for the MonetDB database.
 */
@Slf4j
public class MonetdbSqlDialect extends SqlDialect {

    public static final SqlDialect DEFAULT = new MonetdbSqlDialect(
            EMPTY_CONTEXT
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "\"" )
    );


    /**
     * Creates an MonetdbSqlDialect.
     */
    public MonetdbSqlDialect( Context context ) {
        super( context );
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public boolean supportsColumnNamesWithSchema() {
        return false;
    }


    @Override
    public IntervalParameterStrategy getIntervalParameterStrategy() {
        return IntervalParameterStrategy.MULTIPLICATION;
    }


    @Override
    public boolean supportsBinaryStream() {
        return false;
    }


    @Override
    public SqlNode getCastSpec( AlgDataType type ) {
        String castSpec;
        switch ( type.getPolyType() ) {
            case TINYINT:
                castSpec = "_SMALLINT";
                break;
            case ARRAY:
                // We need to flag the type with a underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_TEXT";
                break;
            case VARBINARY:
                castSpec = "_TEXT";
                break;
            case FILE:
            case IMAGE:
            case VIDEO:
            case AUDIO:
                // We need to flag the type with a underscore to flag the type (the underscore is removed in the unparse method)
                castSpec = "_BLOB";
                break;
            default:
                return super.getCastSpec( type );
        }

        return new SqlDataTypeSpec( new SqlIdentifier( castSpec, ParserPos.ZERO ), -1, -1, null, null, ParserPos.ZERO );
    }


    @Override
    public Expression getExpression( AlgDataType fieldType, Expression child ) {
        return switch ( fieldType.getPolyType() ) {
            case TEXT -> {
                UnaryExpression client = Expressions.convert_( child, MonetClob.class );
                yield super.getExpression( fieldType, Expressions.call( MonetdbSqlDialect.class, "toString", client ) );
            }
            default -> super.getExpression( fieldType, child );
        };
    }


    @SuppressWarnings("unused")
    public static String toString( MonetClob clob ) {
        if ( clob == null ) {
            return null;
        }
        try {
            if ( clob.length() == 0 ) {
                return null;
            }
            return clob.getSubString( 1, (int) clob.length() );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public boolean supportsProject( Project project ) {
        MonetdbRexVisitor visitor = new MonetdbRexVisitor();
        project.getProjects().forEach( p -> p.accept( visitor ) );
        return visitor.supportsRex;
    }


    @Override
    public boolean supportsFilter( Filter filter ) {
        MonetdbRexVisitor visitor = new MonetdbRexVisitor();
        filter.getCondition().accept( visitor );
        return visitor.supportsRex;
    }


    @Override
    public boolean supportsJoin( Join join ) {
        return join.getInputs().stream().noneMatch( AlgNode::containsJoin );// monetdb seems to have an error with nested joins and aliases
    }


    @Override
    public boolean handlesUtcIncorrectly() {
        return true;
    }


    @Override
    public boolean supportsSort( Sort sort ) {
        MonetdbRexVisitor visitor = new MonetdbRexVisitor();
        sort.getChildExps().forEach( n -> n.accept( visitor ) );
        return visitor.supportsRex;
    }


    @Override
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingLimit( writer, offset, fetch );
    }


    @Override
    public boolean supportsIsBoolean() {
        return false;
    }


    @Override
    public boolean supportsComplexBinary() {
        return false;
    }


    @Override
    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.getKind() == Kind.EXTRACT
                && call.getSqlOperandList().size() > 1
                && call.getSqlOperandList().get( 0 ) instanceof SqlLiteral literal
                && literal.value.isSymbol()
                && literal.value.asSymbol().value == TimeUnitRange.DOW ) {
            // monetdb starts the week on monday, so we need to add 1 to the result
            writer.sep( "(" );
            super.unparseCall( writer, call, leftPrec, rightPrec );
            writer.print( " + 1) % 7 " );
            return;
        }

        super.unparseCall( writer, call, leftPrec, rightPrec );
    }


    @Getter
    private static class MonetdbRexVisitor extends RexShuttle {

        boolean supportsRex = true;


        @Override
        public RexNode visitCall( RexCall call ) {
            if ( call.getKind() == Kind.FLOOR ) {
                if ( call.getOperands().size() == 2
                        && call.getOperands().get( 1 ) instanceof RexLiteral type
                        && type.value.isSymbol()
                        && type.value.asSymbol().value instanceof TimeUnitRange ) {
                    supportsRex = false;
                }

            } else if ( call.getKind() == Kind.EXTRACT && call.getOperands().size() >= 2 // second is value to extract from
                    && PolyType.DATETIME_TYPES.contains( call.getOperands().get( 1 ).getType().getPolyType() ) ) {
                supportsRex = false;
            } else if ( call.getKind() == Kind.IS_NOT_TRUE || call.getKind() == Kind.IS_NOT_FALSE ) {
                supportsRex = false;
            } else if ( call.getKind() == Kind.LIKE && call.getOperands().size() == 3 && call.getOperands().get( 2 ) instanceof RexDynamicParam ) {
                supportsRex = false;
            } else if ( call.op.getOperatorName() == OperatorName.OVERLAY ) {
                supportsRex = false;
            } else if ( call.op.getOperatorName() == OperatorName.ATAN2 ) {
                supportsRex = false;
            } else if ( call.op.getOperatorName() == OperatorName.TRIM ) {
                supportsRex = false;
            } else if ( call.op.getOperatorName() == OperatorName.SUBSTRING ) {
                supportsRex = false;
            } else if ( call.op.getOperatorName() == OperatorName.HOUR ) {
                supportsRex = false;
            }
            return super.visitCall( call );
        }

    }

}

