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

package org.polypheny.db.sql.sql;


import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.function.Experimental;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgDataTypeSystemImpl;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.sql.dialect.JethroDataSqlDialect;
import org.polypheny.db.sql.sql.util.SqlBuilder;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * <code>SqlDialect</code> encapsulates the differences between dialects of SQL.
 *
 * It is used by classes such as {@link SqlWriter} and {@link SqlBuilder}.
 */
@Slf4j
public class SqlDialect {

    /**
     * Empty context.
     */
    public static final Context EMPTY_CONTEXT = emptyContext();

    /**
     * Built-in scalar functions and operators common for every dialect.
     */
    protected static final Set<Operator> BUILT_IN_OPERATORS_LIST =
            ImmutableSet.<Operator>builder()
                    .add( OperatorRegistry.get( OperatorName.ABS ) )
                    .add( OperatorRegistry.get( OperatorName.ACOS ) )
                    .add( OperatorRegistry.get( OperatorName.AND ) )
                    .add( OperatorRegistry.get( OperatorName.ASIN ) )
                    .add( OperatorRegistry.get( OperatorName.BETWEEN ) )
                    .add( OperatorRegistry.get( OperatorName.CASE ) )
                    .add( OperatorRegistry.get( OperatorName.CAST ) )
                    .add( OperatorRegistry.get( OperatorName.CEIL ) )
                    .add( OperatorRegistry.get( OperatorName.CHAR_LENGTH ) )
                    .add( OperatorRegistry.get( OperatorName.CHARACTER_LENGTH ) )
                    .add( OperatorRegistry.get( OperatorName.COALESCE ) )
                    .add( OperatorRegistry.get( OperatorName.CONCAT ) )
                    .add( OperatorRegistry.get( OperatorName.COS ) )
                    .add( OperatorRegistry.get( OperatorName.COT ) )
                    .add( OperatorRegistry.get( OperatorName.DIVIDE ) )
                    .add( OperatorRegistry.get( OperatorName.EQUALS ) )
                    .add( OperatorRegistry.get( OperatorName.FLOOR ) )
                    .add( OperatorRegistry.get( OperatorName.GREATER_THAN ) )
                    .add( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ) )
                    .add( OperatorRegistry.get( OperatorName.IN ) )
                    .add( OperatorRegistry.get( OperatorName.IS_NOT_NULL ) )
                    .add( OperatorRegistry.get( OperatorName.IS_NULL ) )
                    .add( OperatorRegistry.get( OperatorName.LESS_THAN ) )
                    .add( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ) )
                    .add( OperatorRegistry.get( OperatorName.LIKE ) )
                    .add( OperatorRegistry.get( OperatorName.LN ) )
                    .add( OperatorRegistry.get( OperatorName.LOG10 ) )
                    .add( OperatorRegistry.get( OperatorName.MINUS ) )
                    .add( OperatorRegistry.get( OperatorName.MOD ) )
                    .add( OperatorRegistry.get( OperatorName.MULTIPLY ) )
                    .add( OperatorRegistry.get( OperatorName.NOT ) )
                    .add( OperatorRegistry.get( OperatorName.NOT_BETWEEN ) )
                    .add( OperatorRegistry.get( OperatorName.NOT_EQUALS ) )
                    .add( OperatorRegistry.get( OperatorName.NOT_IN ) )
                    .add( OperatorRegistry.get( OperatorName.NOT_LIKE ) )
                    .add( OperatorRegistry.get( OperatorName.OR ) )
                    .add( OperatorRegistry.get( OperatorName.PI ) )
                    .add( OperatorRegistry.get( OperatorName.PLUS ) )
                    .add( OperatorRegistry.get( OperatorName.POWER ) )
                    .add( OperatorRegistry.get( OperatorName.RAND ) )
                    .add( OperatorRegistry.get( OperatorName.ROUND ) )
                    .add( OperatorRegistry.get( OperatorName.ROW ) )
                    .add( OperatorRegistry.get( OperatorName.SIN ) )
                    .add( OperatorRegistry.get( OperatorName.SQRT ) )
                    .add( OperatorRegistry.get( OperatorName.SUBSTRING ) )
                    .add( OperatorRegistry.get( OperatorName.TAN ) )
                    .build();


    private final String identifierQuoteString;
    private final String identifierEndQuoteString;
    private final String identifierEscapedQuote;
    private final DatabaseProduct databaseProduct;
    protected final NullCollation nullCollation;
    private final AlgDataTypeSystem dataTypeSystem;


    /**
     * Creates a SqlDialect.
     *
     * @param context All the information necessary to create a dialect
     */
    public SqlDialect( Context context ) {
        this.nullCollation = Objects.requireNonNull( context.nullCollation() );
        this.dataTypeSystem = Objects.requireNonNull( context.dataTypeSystem() );
        this.databaseProduct = Objects.requireNonNull( context.databaseProduct() );
        String identifierQuoteString = context.identifierQuoteString();
        if ( identifierQuoteString != null ) {
            identifierQuoteString = identifierQuoteString.trim();
            if ( identifierQuoteString.equals( "" ) ) {
                identifierQuoteString = null;
            }
        }
        this.identifierQuoteString = identifierQuoteString;
        this.identifierEndQuoteString =
                identifierQuoteString == null
                        ? null
                        : identifierQuoteString.equals( "[" )
                                ? "]"
                                : identifierQuoteString;
        this.identifierEscapedQuote =
                identifierQuoteString == null
                        ? null
                        : this.identifierEndQuoteString + this.identifierEndQuoteString;
    }


    /**
     * Creates an empty context. Use {@link #EMPTY_CONTEXT} if possible.
     */
    protected static Context emptyContext() {
        return new ContextImpl(
                DatabaseProduct.UNKNOWN,
                null,
                null,
                -1,
                -1,
                null,
                NullCollation.HIGH,
                AlgDataTypeSystemImpl.DEFAULT,
                JethroDataSqlDialect.JethroInfo.EMPTY );
    }


    /**
     * Returns the type system implementation for this dialect.
     */
    public AlgDataTypeSystem getTypeSystem() {
        return dataTypeSystem;
    }


    /**
     * Encloses an identifier in quotation marks appropriate for the current SQL dialect.
     *
     * For example, <code>quoteIdentifier("emp")</code> yields a string containing <code>"emp"</code> in Oracle, and a string containing
     * <code> [emp]</code> in Access.
     *
     * @param val Identifier to quote
     * @return Quoted identifier
     */
    public String quoteIdentifier( String val ) {
        if ( identifierQuoteString == null ) {
            return val; // quoting is not supported
        }
        String val2 = val.replaceAll( identifierEndQuoteString, identifierEscapedQuote );
        return identifierQuoteString + val2 + identifierEndQuoteString;
    }


    /**
     * Encloses an identifier in quotation marks appropriate for the current SQL dialect, writing the result to a {@link StringBuilder}.
     *
     * For example, <code>quoteIdentifier("emp")</code> yields a string containing <code>"emp"</code> in Oracle, and a string containing
     * <code>[emp]</code> in Access.
     *
     * @param buf Buffer
     * @param val Identifier to quote
     * @return The buffer
     */
    public StringBuilder quoteIdentifier( StringBuilder buf, String val ) {
        if ( identifierQuoteString == null ) {
            buf.append( val ); // quoting is not supported
            return buf;
        }
        String val2 = val.replaceAll( identifierEndQuoteString, identifierEscapedQuote );
        buf.append( identifierQuoteString );
        buf.append( val2 );
        buf.append( identifierEndQuoteString );
        return buf;
    }


    /**
     * Quotes a multi-part identifier.
     *
     * @param buf Buffer
     * @param identifiers List of parts of the identifier to quote
     * @return The buffer
     */
    public StringBuilder quoteIdentifier( StringBuilder buf, List<String> identifiers ) {
        int i = 0;
        for ( String identifier : identifiers ) {
            if ( i != 0 || identifiers.size() != 3 || this.supportsColumnNamesWithSchema() ) {
                if ( i > 0 ) {
                    buf.append( '.' );
                }
                quoteIdentifier( buf, identifier );
            }
            i++;
        }
        return buf;
    }


    /**
     * Returns whether a given identifier needs to be quoted.
     */
    public boolean identifierNeedsToBeQuoted( String val ) {
        return !Pattern.compile( "^[A-Z_$0-9]+" ).matcher( val ).matches();
    }


    /**
     * Converts a string into a string literal. For example, <code>can't run</code> becomes <code>'can''t run'</code>.
     */
    public String quoteStringLiteral( String val ) {
        if ( containsNonAscii( val ) ) {
            final StringBuilder buf = new StringBuilder();
            quoteStringLiteralUnicode( buf, val );
            return buf.toString();
        } else {
            val = FakeUtil.replace( val, "'", "''" );
            return "'" + val + "'";
        }
    }


    public void unparseCall( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        ((SqlOperator) call.getOperator()).unparse( writer, call, leftPrec, rightPrec );
    }


    public void unparseDateTimeLiteral( SqlWriter writer, SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec ) {
        writer.literal( literal.toString() );
    }


    public void unparseSqlDatetimeArithmetic( SqlWriter writer, SqlCall call, Kind Kind, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.sep( (Kind.PLUS == Kind) ? "+" : "-" );
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, rightPrec );
        writer.endList( frame );
        //Only two parameters are present normally
        //Checking parameter count to prevent errors
        if ( call.getOperandList().size() > 2 ) {
            ((SqlNode) call.operand( 2 )).unparse( writer, leftPrec, rightPrec );
        }
    }


    /**
     * Converts an interval qualifier to a SQL string. The default implementation returns strings such as
     * <code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code>.
     */
    public void unparseSqlIntervalQualifier( SqlWriter writer, SqlIntervalQualifier qualifier, AlgDataTypeSystem typeSystem ) {
        final String start = qualifier.timeUnitRange.startUnit.name();
        final int fractionalSecondPrecision = qualifier.getFractionalSecondPrecision( typeSystem );
        final int startPrecision = qualifier.getStartPrecision( typeSystem );
        if ( qualifier.timeUnitRange.startUnit == TimeUnit.SECOND ) {
            if ( !qualifier.useDefaultFractionalSecondPrecision() ) {
                final SqlWriter.Frame frame = writer.startFunCall( start );
                writer.print( startPrecision );
                writer.sep( ",", true );
                writer.print( qualifier.getFractionalSecondPrecision( typeSystem ) );
                writer.endList( frame );
            } else if ( !qualifier.useDefaultStartPrecision() ) {
                final SqlWriter.Frame frame = writer.startFunCall( start );
                writer.print( startPrecision );
                writer.endList( frame );
            } else {
                writer.keyword( start );
            }
        } else {
            if ( !qualifier.useDefaultStartPrecision() ) {
                final SqlWriter.Frame frame = writer.startFunCall( start );
                writer.print( startPrecision );
                writer.endList( frame );
            } else {
                writer.keyword( start );
            }

            if ( null != qualifier.timeUnitRange.endUnit ) {
                writer.keyword( "TO" );
                final String end = qualifier.timeUnitRange.endUnit.name();
                if ( (TimeUnit.SECOND == qualifier.timeUnitRange.endUnit) && (!qualifier.useDefaultFractionalSecondPrecision()) ) {
                    final SqlWriter.Frame frame = writer.startFunCall( end );
                    writer.print( fractionalSecondPrecision );
                    writer.endList( frame );
                } else {
                    writer.keyword( end );
                }
            }
        }
    }


    /**
     * Converts an interval literal to a SQL string. The default implementation returns strings such as
     * <code>INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)</code>.
     */
    public void unparseSqlIntervalLiteral( SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec ) {
        SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue) literal.getValue();
        writer.keyword( "INTERVAL" );
        if ( interval.getSign() == -1 ) {
            writer.print( "-" );
        }
        writer.literal( "'" + literal.getValue().toString() + "'" );
        unparseSqlIntervalQualifier( writer, interval.getIntervalQualifier(), AlgDataTypeSystem.DEFAULT );
    }


    /**
     * Returns whether the string contains any characters outside the comfortable 7-bit ASCII range (32 through 127).
     *
     * @param s String
     * @return Whether string contains any non-7-bit-ASCII characters
     */
    private static boolean containsNonAscii( String s ) {
        for ( int i = 0; i < s.length(); i++ ) {
            char c = s.charAt( i );
            if ( c < 32 || c >= 128 ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Converts a string into a unicode string literal. For example,
     * <code>can't{tab}run\</code> becomes <code>u'can''t\0009run\\'</code>.
     */
    public void quoteStringLiteralUnicode( StringBuilder buf, String val ) {
        buf.append( "u&'" );
        for ( int i = 0; i < val.length(); i++ ) {
            char c = val.charAt( i );
            if ( c < 32 || c >= 128 ) {
                buf.append( '\\' );
                buf.append( HEXITS[(c >> 12) & 0xf] );
                buf.append( HEXITS[(c >> 8) & 0xf] );
                buf.append( HEXITS[(c >> 4) & 0xf] );
                buf.append( HEXITS[c & 0xf] );
            } else if ( c == '\'' || c == '\\' ) {
                buf.append( c );
                buf.append( c );
            } else {
                buf.append( c );
            }
        }
        buf.append( "'" );
    }


    private static final char[] HEXITS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };


    /**
     * Converts a string literal back into a string. For example, <code>'can''t run'</code> becomes <code>can't run</code>.
     */
    public String unquoteStringLiteral( String val ) {
        if ( (val != null)
                && (val.charAt( 0 ) == '\'')
                && (val.charAt( val.length() - 1 ) == '\'') ) {
            if ( val.length() > 2 ) {
                val = FakeUtil.replace( val, "''", "'" );
                return val.substring( 1, val.length() - 1 );
            } else {
                // zero length string
                return "";
            }
        }
        return val;
    }


    protected boolean allowsAs() {
        return true;
    }


    // -- behaviors --
    protected boolean requiresAliasForFromItems() {
        return false;
    }


    /**
     * Returns whether a qualified table in the FROM clause has an implicit alias which consists of just the table name.
     *
     * For example, in {@link DatabaseProduct#ORACLE}
     *
     * <blockquote>SELECT * FROM sales.emp</blockquote>
     *
     * is equivalent to
     *
     * <blockquote>SELECT * FROM sales.emp AS emp</blockquote>
     *
     * and therefore
     *
     * <blockquote>SELECT emp.empno FROM sales.emp</blockquote>
     *
     * is valid. But {@link DatabaseProduct#DB2} does not have an implicit alias, so the previous query it not valid; you need to write
     *
     * <blockquote>SELECT sales.emp.empno FROM sales.emp</blockquote>
     *
     * Returns true for all databases except DB2.
     */
    public boolean hasImplicitTableAlias() {
        return true;
    }


    /**
     * Converts a timestamp to a SQL timestamp literal, e.g. {@code TIMESTAMP '2009-12-17 12:34:56'}.
     *
     * Timestamp values do not have a time zone. We therefore interpret them as the number of milliseconds after the UTC epoch, and the formatted
     * value is that time in UTC.
     *
     * In particular,
     *
     * <blockquote><code>quoteTimestampLiteral(new Timestamp(0));</code></blockquote>
     *
     * returns {@code TIMESTAMP '1970-01-01 00:00:00'}, regardless of the JVM's time zone.
     *
     * @param timestamp Timestamp
     * @return SQL timestamp literal
     */
    public String quoteTimestampLiteral( Timestamp timestamp ) {
        final SimpleDateFormat format = new SimpleDateFormat( "'TIMESTAMP' ''yyyy-MM-DD HH:mm:SS''", Locale.ROOT );
        format.setTimeZone( DateTimeUtils.UTC_ZONE );
        return format.format( timestamp );
    }


    /**
     * Returns whether the dialect supports character set names as part of a data type, for instance {@code VARCHAR(30) CHARACTER SET `ISO-8859-1`}.
     */
    public boolean supportsCharSet() {
        return true;
    }


    public boolean supportsAggregateFunction( Kind kind ) {
        switch ( kind ) {
            case COUNT:
            case SUM:
            case SUM0:
            case MIN:
            case MAX:
                return true;
        }
        return false;
    }


    /**
     * Returns whether this dialect supports window functions (OVER clause).
     */
    public boolean supportsWindowFunctions() {
        return true;
    }


    /**
     * Returns whether this dialect supports nested arrays
     */
    public boolean supportsNestedArrays() {
        return false;
    }


    /**
     * Returns whether this dialect supports column names of level 3 (SchemaName.TableName.ColumnName).
     */
    public boolean supportsColumnNamesWithSchema() {
        return true;
    }


    /**
     * Returns whether this dialect supports a given function or operator.
     * It only applies to built-in scalar functions and operators, since user-defined functions and procedures should be read by JdbcSchema.
     */
    public boolean supportsFunction( SqlOperator operator, AlgDataType type, List<AlgDataType> paramTypes ) {
        switch ( operator.kind ) {
            case AND:
            case BETWEEN:
            case CASE:
            case CAST:
            case CEIL:
            case COALESCE:
            case DIVIDE:
            case EQUALS:
            case FLOOR:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IN:
            case IS_NULL:
            case IS_NOT_NULL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case MINUS:
            case MOD:
            case NOT:
            case NOT_IN:
            case NOT_EQUALS:
            case NVL:
            case OR:
            case PLUS:
            case ROW:
            case TIMES:
                return true;
            default:
                return BUILT_IN_OPERATORS_LIST.contains( operator );
        }
    }


    public CalendarPolicy getCalendarPolicy() {
        return CalendarPolicy.NULL;
    }


    public SqlNode getCastSpec( AlgDataType type ) {
        if ( type instanceof BasicPolyType ) {
            int precision = type.getPrecision();
            if ( type.getPolyType() == PolyType.JSON ) {
                precision = 2024;
                type = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT ).createPolyType( PolyType.VARCHAR, precision );
            }
            switch ( type.getPolyType() ) {
                case JSON:
                case VARCHAR:
                    // if needed, adjust varchar length to max length supported by the system
                    int maxPrecision = getTypeSystem().getMaxPrecision( type.getPolyType() );
                    if ( type.getPrecision() > maxPrecision ) {
                        precision = maxPrecision;
                    }
            }
            return new SqlDataTypeSpec(
                    new SqlIdentifier( type.getPolyType().name(), ParserPos.ZERO ),
                    precision,
                    type.getScale(),
                    null,
                    ///type.getCharset() != null && supportsCharSet()
                    //        ? type.getCharset().name()
                    //        : null,
                    null,
                    ParserPos.ZERO );
        }
        return (SqlNode) PolyTypeUtil.convertTypeToSpec( type );
    }


    /**
     * Rewrite SINGLE_VALUE into expression based on database variants
     * E.g. HSQLDB, MYSQL, ORACLE, etc
     */
    public SqlNode rewriteSingleValueExpr( SqlNode aggCall ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "SINGLE_VALUE rewrite not supported for {}", databaseProduct );
        }
        return aggCall;
    }


    public String timeUnitName( TimeUnitRange unit ) {
        return unit.name();
    }


    /**
     * Returns the SqlNode for emulating the null direction for the given field or <code>null</code> if no emulation needs to be done.
     *
     * @param node The SqlNode representing the expression
     * @param nullsFirst Whether nulls should come first
     * @param desc Whether the sort direction is {@link AlgFieldCollation.Direction#DESCENDING} or {@link AlgFieldCollation.Direction#STRICTLY_DESCENDING}
     * @return A SqlNode for null direction emulation or <code>null</code> if not required
     */
    public SqlNode emulateNullDirection( SqlNode node, boolean nullsFirst, boolean desc ) {
        return null;
    }


    protected SqlNode emulateNullDirectionWithIsNull( SqlNode node, boolean nullsFirst, boolean desc ) {
        // No need for emulation if the nulls will anyways come out the way we want them based on "nullsFirst" and "desc".
        if ( nullCollation.isDefaultOrder( nullsFirst, desc ) ) {
            return null;
        }

        node = (SqlNode) OperatorRegistry.get( OperatorName.IS_NULL ).createCall( ParserPos.ZERO, node );
        if ( nullsFirst ) {
            node = (SqlNode) OperatorRegistry.get( OperatorName.DESC ).createCall( ParserPos.ZERO, node );
        }
        return node;
    }


    /**
     * Converts an offset and fetch into SQL.
     *
     * At least one of {@code offset} and {@code fetch} must be provided.
     *
     * Common options:
     * <ul>
     * <li>{@code OFFSET offset ROWS FETCH NEXT fetch ROWS ONLY} (ANSI standard SQL, Oracle, PostgreSQL, and the default)</li>
     * <li>{@code LIMIT fetch OFFSET offset} (Apache Hive, MySQL, Redshift)</li>
     * </ul>
     *
     * @param writer Writer
     * @param offset Number of rows to skip before emitting, or null
     * @param fetch Number of rows to fetch, or null
     * @see #unparseFetchUsingAnsi(SqlWriter, SqlNode, SqlNode)
     * @see #unparseFetchUsingLimit(SqlWriter, SqlNode, SqlNode)
     */
    public void unparseOffsetFetch( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        unparseFetchUsingAnsi( writer, offset, fetch );
    }


    /**
     * Unparses offset/fetch using ANSI standard "OFFSET offset ROWS FETCH NEXT fetch ROWS ONLY" syntax.
     */
    protected final void unparseFetchUsingAnsi( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        Preconditions.checkArgument( fetch != null || offset != null );
        if ( offset != null ) {
            writer.newlineAndIndent();
            final SqlWriter.Frame offsetFrame = writer.startList( SqlWriter.FrameTypeEnum.OFFSET );
            writer.keyword( "OFFSET" );
            offset.unparse( writer, -1, -1 );
            writer.keyword( "ROWS" );
            writer.endList( offsetFrame );
        }
        if ( fetch != null ) {
            writer.newlineAndIndent();
            final SqlWriter.Frame fetchFrame = writer.startList( SqlWriter.FrameTypeEnum.FETCH );
            writer.keyword( "FETCH" );
            writer.keyword( "NEXT" );
            fetch.unparse( writer, -1, -1 );
            writer.keyword( "ROWS" );
            writer.keyword( "ONLY" );
            writer.endList( fetchFrame );
        }
    }


    /**
     * Unparses offset/fetch using "LIMIT fetch OFFSET offset" syntax.
     */
    protected final void unparseFetchUsingLimit( SqlWriter writer, SqlNode offset, SqlNode fetch ) {
        Preconditions.checkArgument( fetch != null || offset != null );
        if ( fetch != null ) {
            writer.newlineAndIndent();
            final SqlWriter.Frame fetchFrame = writer.startList( SqlWriter.FrameTypeEnum.FETCH );
            writer.keyword( "LIMIT" );
            fetch.unparse( writer, -1, -1 );
            writer.endList( fetchFrame );
        }
        if ( offset != null ) {
            writer.newlineAndIndent();
            final SqlWriter.Frame offsetFrame = writer.startList( SqlWriter.FrameTypeEnum.OFFSET );
            writer.keyword( "OFFSET" );
            offset.unparse( writer, -1, -1 );
            writer.endList( offsetFrame );
        }
    }


    /**
     * Returns whether the dialect supports nested aggregations, for instance {@code SELECT SUM(SUM(1)) }.
     */
    public boolean supportsNestedAggregations() {
        return true;
    }


    /**
     * Returns how NULL values are sorted if an ORDER BY item does not contain NULLS ASCENDING or NULLS DESCENDING.
     */
    public NullCollation getNullCollation() {
        return nullCollation;
    }


    /**
     * Returns whether NULL values are sorted first or last, in this dialect, in an ORDER BY item of a given direction.
     */
    public AlgFieldCollation.NullDirection defaultNullDirection( AlgFieldCollation.Direction direction ) {
        switch ( direction ) {
            case ASCENDING:
            case STRICTLY_ASCENDING:
                return getNullCollation().last( false )
                        ? AlgFieldCollation.NullDirection.LAST
                        : AlgFieldCollation.NullDirection.FIRST;
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return getNullCollation().last( true )
                        ? AlgFieldCollation.NullDirection.LAST
                        : AlgFieldCollation.NullDirection.FIRST;
            default:
                return AlgFieldCollation.NullDirection.UNSPECIFIED;
        }
    }


    /**
     * Returns whether the dialect supports VALUES in a sub-query with and an "AS t(column, ...)" values to define column names.
     *
     * Currently, only Oracle does not. For this, we generate "SELECT v0 AS c0, v1 AS c1 ... UNION ALL ...". We may need to refactor this method when we
     * support VALUES for other dialects.
     */
    @Experimental
    public boolean supportsAliasedValues() {
        return true;
    }


    public String getArrayComponentTypeString( SqlType type ) {
        return type.name();
    }


    public boolean supportsBinaryStream() {
        return true;
    }


    public boolean supportsIsBoolean() {
        return true;
    }


    public enum IntervalParameterStrategy {CAST, MULTIPLICATION, NONE}


    public IntervalParameterStrategy getIntervalParameterStrategy() {
        return IntervalParameterStrategy.CAST;
    }


    /**
     * A few utility functions copied from org.polypheny.db.util.Util. We have copied them because we wish to keep SqlDialect's dependencies to a minimum.
     */
    public static class FakeUtil {

        public static Error newInternal( Throwable e, String s ) {
            String message = "Internal error: \u0000" + s;
            AssertionError ae = new AssertionError( message );
            ae.initCause( e );
            return ae;
        }


        /**
         * Replaces every occurrence of <code>find</code> in <code>s</code> with <code>replace</code>.
         */
        public static String replace( String s, String find, String replace ) {
            // let's be optimistic
            int found = s.indexOf( find );
            if ( found == -1 ) {
                return s;
            }
            StringBuilder sb = new StringBuilder( s.length() );
            int start = 0;
            for ( ; ; ) {
                for ( ; start < found; start++ ) {
                    sb.append( s.charAt( start ) );
                }
                if ( found == s.length() ) {
                    break;
                }
                sb.append( replace );
                start += find.length();
                found = s.indexOf( find, start );
                if ( found == -1 ) {
                    found = s.length();
                }
            }
            return sb.toString();
        }

    }


    /**
     * Whether this JDBC driver needs you to pass a Calendar object to methods such as {@link ResultSet#getTimestamp(int, java.util.Calendar)}.
     */
    public enum CalendarPolicy {
        NONE,
        NULL,
        LOCAL,
        DIRECT,
        SHIFT
    }


    /**
     * Rough list of flavors of database.
     *
     * These values cannot help you distinguish between features that exist in different versions or ports of a database, but they are sufficient
     * to drive a {@code switch} statement if behavior is broadly different between say, MySQL and Oracle.
     *
     * If possible, you should not refer to particular database at all; write extend the dialect to describe the particular capability, for example,
     * whether the database allows expressions to appear in the GROUP BY clause.
     */
    public enum DatabaseProduct {
        ACCESS( "Access", "\"", NullCollation.HIGH ),
        BIG_QUERY( "Google BigQuery", "`", NullCollation.LOW ),
        POLYPHENYDB( "Polypheny-DB", "\"", NullCollation.HIGH ),
        MSSQL( "Microsoft SQL Server", "[", NullCollation.HIGH ),
        MYSQL( "MySQL", "`", NullCollation.LOW ),
        ORACLE( "Oracle", "\"", NullCollation.HIGH ),
        DERBY( "Apache Derby", null, NullCollation.HIGH ),
        DB2( "IBM DB2", null, NullCollation.HIGH ),
        FIREBIRD( "Firebird", null, NullCollation.HIGH ),
        H2( "H2", "\"", NullCollation.HIGH ),
        HIVE( "Apache Hive", null, NullCollation.LOW ),
        INFORMIX( "Informix", null, NullCollation.HIGH ),
        INGRES( "Ingres", null, NullCollation.HIGH ),
        JETHRO( "JethroData", "\"", NullCollation.LOW ),
        LUCIDDB( "LucidDB", "\"", NullCollation.HIGH ),
        INTERBASE( "Interbase", null, NullCollation.HIGH ),
        PHOENIX( "Phoenix", "\"", NullCollation.HIGH ),
        POSTGRESQL( "PostgreSQL", "\"", NullCollation.HIGH ),
        NETEZZA( "Netezza", "\"", NullCollation.HIGH ),
        INFOBRIGHT( "Infobright", "`", NullCollation.HIGH ),
        NEOVIEW( "Neoview", null, NullCollation.HIGH ),
        SYBASE( "Sybase", null, NullCollation.HIGH ),
        TERADATA( "Teradata", "\"", NullCollation.HIGH ),
        HSQLDB( "Hsqldb", null, NullCollation.HIGH ),
        VERTICA( "Vertica", "\"", NullCollation.HIGH ),
        SQLSTREAM( "SQLstream", "\"", NullCollation.HIGH ),
        MONETDB( "MonetDB", "\"", NullCollation.HIGH ),

        /**
         * Paraccel, now called Actian Matrix. Redshift is based on this, so presumably the dialect capabilities are similar.
         */
        PARACCEL( "Paraccel", "\"", NullCollation.HIGH ),
        REDSHIFT( "Redshift", "\"", NullCollation.HIGH ),

        /**
         * Placeholder for the unknown database.
         *
         * Its dialect is useful for generating generic SQL. If you need to do something database-specific like quoting identifiers, don't rely
         * on this dialect to do what you want.
         */
        UNKNOWN( "Unknown", "`", NullCollation.HIGH );

        private final Supplier<SqlDialect> dialect;


        DatabaseProduct( String databaseProductName, String quoteString, NullCollation nullCollation ) {
            Objects.requireNonNull( databaseProductName );
            Objects.requireNonNull( nullCollation );
            dialect = Suppliers.memoize( () -> {
                final SqlDialect dialect = SqlDialectFactoryImpl.simple( DatabaseProduct.this );
                if ( dialect != null ) {
                    return dialect;
                }
                return new SqlDialect( SqlDialect.EMPTY_CONTEXT
                        .withDatabaseProduct( DatabaseProduct.this )
                        .withDatabaseProductName( databaseProductName )
                        .withIdentifierQuoteString( quoteString )
                        .withNullCollation( nullCollation ) );
            } );
        }


        /**
         * Returns a dummy dialect for this database.
         *
         * Since databases have many versions and flavors, this dummy dialect is at best an approximation. If you want exact information, better to
         * use a dialect created from an actual connection's metadata (see {@link SqlDialectFactory#create(java.sql.DatabaseMetaData)}).
         *
         * @return Dialect representing lowest-common-denominator behavior for all versions of this database
         */
        public SqlDialect getDialect() {
            return dialect.get();
        }
    }


    /**
     * Information for creating a dialect.
     *
     * It is immutable; to "set" a property, call one of the "with" methods, which returns a new context with the desired property value.
     */
    public interface Context {

        @Nonnull
        DatabaseProduct databaseProduct();

        Context withDatabaseProduct( @Nonnull DatabaseProduct databaseProduct );

        String databaseProductName();

        Context withDatabaseProductName( String databaseProductName );

        String databaseVersion();

        Context withDatabaseVersion( String databaseVersion );

        int databaseMajorVersion();

        Context withDatabaseMajorVersion( int databaseMajorVersion );

        int databaseMinorVersion();

        Context withDatabaseMinorVersion( int databaseMinorVersion );

        String identifierQuoteString();

        Context withIdentifierQuoteString( String identifierQuoteString );

        @Nonnull
        NullCollation nullCollation();

        Context withNullCollation( @Nonnull NullCollation nullCollation );

        @Nonnull
        AlgDataTypeSystem dataTypeSystem();

        Context withDataTypeSystem( @Nonnull AlgDataTypeSystem dataTypeSystem );

        JethroDataSqlDialect.JethroInfo jethroInfo();

        Context withJethroInfo( JethroDataSqlDialect.JethroInfo jethroInfo );

    }


    /**
     * Implementation of Context.
     */
    private static class ContextImpl implements Context {

        private final DatabaseProduct databaseProduct;
        private final String databaseProductName;
        private final String databaseVersion;
        private final int databaseMajorVersion;
        private final int databaseMinorVersion;
        private final String identifierQuoteString;
        private final NullCollation nullCollation;
        private final AlgDataTypeSystem dataTypeSystem;
        private final JethroDataSqlDialect.JethroInfo jethroInfo;


        private ContextImpl(
                DatabaseProduct databaseProduct,
                String databaseProductName,
                String databaseVersion,
                int databaseMajorVersion,
                int databaseMinorVersion,
                String identifierQuoteString,
                NullCollation nullCollation,
                AlgDataTypeSystem dataTypeSystem,
                JethroDataSqlDialect.JethroInfo jethroInfo ) {
            this.databaseProduct = Objects.requireNonNull( databaseProduct );
            this.databaseProductName = databaseProductName;
            this.databaseVersion = databaseVersion;
            this.databaseMajorVersion = databaseMajorVersion;
            this.databaseMinorVersion = databaseMinorVersion;
            this.identifierQuoteString = identifierQuoteString;
            this.nullCollation = Objects.requireNonNull( nullCollation );
            this.dataTypeSystem = Objects.requireNonNull( dataTypeSystem );
            this.jethroInfo = Objects.requireNonNull( jethroInfo );
        }


        @Override
        @Nonnull
        public DatabaseProduct databaseProduct() {
            return databaseProduct;
        }


        @Override
        public Context withDatabaseProduct( @Nonnull DatabaseProduct databaseProduct ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        public String databaseProductName() {
            return databaseProductName;
        }


        @Override
        public Context withDatabaseProductName( String databaseProductName ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        public String databaseVersion() {
            return databaseVersion;
        }


        @Override
        public Context withDatabaseVersion( String databaseVersion ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        public int databaseMajorVersion() {
            return databaseMajorVersion;
        }


        @Override
        public Context withDatabaseMajorVersion( int databaseMajorVersion ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        public int databaseMinorVersion() {
            return databaseMinorVersion;
        }


        @Override
        public Context withDatabaseMinorVersion( int databaseMinorVersion ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        public String identifierQuoteString() {
            return identifierQuoteString;
        }


        @Override
        public Context withIdentifierQuoteString( String identifierQuoteString ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        @Nonnull
        public NullCollation nullCollation() {
            return nullCollation;
        }


        @Override
        public Context withNullCollation( @Nonnull NullCollation nullCollation ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        @Nonnull
        public AlgDataTypeSystem dataTypeSystem() {
            return dataTypeSystem;
        }


        @Override
        public Context withDataTypeSystem( @Nonnull AlgDataTypeSystem dataTypeSystem ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }


        @Override
        @Nonnull
        public JethroDataSqlDialect.JethroInfo jethroInfo() {
            return jethroInfo;
        }


        @Override
        public Context withJethroInfo( JethroDataSqlDialect.JethroInfo jethroInfo ) {
            return new ContextImpl(
                    databaseProduct,
                    databaseProductName,
                    databaseVersion,
                    databaseMajorVersion,
                    databaseMinorVersion,
                    identifierQuoteString,
                    nullCollation,
                    dataTypeSystem,
                    jethroInfo );
        }

    }

}
