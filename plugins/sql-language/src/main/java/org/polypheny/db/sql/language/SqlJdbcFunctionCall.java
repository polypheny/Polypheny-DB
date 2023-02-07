/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language;


import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.fun.SqlTrimFunction;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Static;


/**
 * A <code>SqlJdbcFunctionCall</code> is a node of a parse tree which represents a JDBC function call. A JDBC call is of the form <code>{fn NAME(arg0, arg1, ...)}</code>.
 *
 * See <a href="http://java.sun.com/products/jdbc/driverdevs.html">Sun's documentation for writers of JDBC drivers</a>.
 *
 * <table>
 * <caption>Supported JDBC functions</caption>
 * <tr>
 * <th>Function Name</th>
 * <th>Function Returns</th>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h3>NUMERIC FUNCTIONS</h3>
 * </td>
 * </tr>
 * <tr>
 * <td>ABS(number)</td>
 * <td>Absolute value of number</td>
 * </tr>
 * <tr>
 * <td>ACOS(float)</td>
 * <td>Arccosine, in radians, of float</td>
 * </tr>
 * <tr>
 * <td>ASIN(float)</td>
 * <td>Arcsine, in radians, of float</td>
 * </tr>
 * <tr>
 * <td>ATAN(float)</td>
 * <td>Arctangent, in radians, of float</td>
 * </tr>
 * <tr>
 * <td>ATAN2(float1, float2)</td>
 * <td>Arctangent, in radians, of float2 / float1</td>
 * </tr>
 * <tr>
 * <td>CEILING(number)</td>
 * <td>Smallest integer &gt;= number</td>
 * </tr>
 * <tr>
 * <td>COS(float)</td>
 * <td>Cosine of float radians</td>
 * </tr>
 * <tr>
 * <td>COT(float)</td>
 * <td>Cotangent of float radians</td>
 * </tr>
 * <tr>
 * <td>DEGREES(number)</td>
 * <td>Degrees in number radians</td>
 * </tr>
 * <tr>
 * <td>EXP(float)</td>
 * <td>Exponential function of float</td>
 * </tr>
 * <tr>
 * <td>FLOOR(number)</td>
 * <td>Largest integer &lt;= number</td>
 * </tr>
 * <tr>
 * <td>LOG(float)</td>
 * <td>Base e logarithm of float</td>
 * </tr>
 * <tr>
 * <td>LOG10(float)</td>
 * <td>Base 10 logarithm of float</td>
 * </tr>
 * <tr>
 * <td>MOD(integer1, integer2)</td>
 * <td>Rh3ainder for integer1 / integer2</td>
 * </tr>
 * <tr>
 * <td>PI()</td>
 * <td>The constant pi</td>
 * </tr>
 * <tr>
 * <td>POWER(number, power)</td>
 * <td>number raised to (integer) power</td>
 * </tr>
 * <tr>
 * <td>RADIANS(number)</td>
 * <td>Radians in number degrees</td>
 * </tr>
 * <tr>
 * <td>RAND(integer)</td>
 * <td>Random floating point for seed integer</td>
 * </tr>
 * <tr>
 * <td>ROUND(number, places)</td>
 * <td>number rounded to places places</td>
 * </tr>
 * <tr>
 * <td>SIGN(number)</td>
 * <td>-1 to indicate number is &lt; 0; 0 to indicate number is = 0; 1 to indicate number is &gt; 0</td>
 * </tr>
 * <tr>
 * <td>SIN(float)</td>
 * <td>Sine of float radians</td>
 * </tr>
 * <tr>
 * <td>SQRT(float)</td>
 * <td>Square root of float</td>
 * </tr>
 * <tr>
 * <td>TAN(float)</td>
 * <td>Tangent of float radians</td>
 * </tr>
 * <tr>
 * <td>TRUNCATE(number, places)</td>
 * <td>number truncated to places places</td>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h3>STRING FUNCTIONS</h3>
 * </td>
 * </tr>
 * <tr>
 * <td>ASCII(string)</td>
 * <td>Integer representing the ASCII code value of the leftmost character in string</td>
 * </tr>
 * <tr>
 * <td>CHAR(code)</td>
 * <td>Character with ASCII code value code, where code is between 0 and 255</td>
 * </tr>
 * <tr>
 * <td>CONCAT(string1, string2)</td>
 * <td>Character string formed by appending string2 to string1; if a string is null, the result is DBMS-dependent</td>
 * </tr>
 * <tr>
 * <td>DIFFERENCE(string1, string2)</td>
 * <td>Integer indicating the difference between the values returned by the function SOUNDEX for string1 and string2</td>
 * </tr>
 * <tr>
 * <td>INSERT(string1, start, length, string2)</td>
 * <td>A character string formed by deleting length characters from string1 beginning at start, and inserting string2 into string1 at start</td>
 * </tr>
 * <tr>
 * <td>LCASE(string)</td>
 * <td>Converts all uppercase characters in string to lowercase</td>
 * </tr>
 * <tr>
 * <td>LEFT(string, count)</td>
 * <td>The count leftmost characters from string</td>
 * </tr>
 * <tr>
 * <td>LENGTH(string)</td>
 * <td>Number of characters in string, excluding trailing blanks</td>
 * </tr>
 * <tr>
 * <td>LOCATE(string1, string2[, start])</td>
 * <td>Position in string2 of the first occurrence of string1, searching from the beginning of string2; if start is specified, the search begins from position start. 0 is returned if string2 does not contain string1. Position 1 is the first character in string2.</td>
 * </tr>
 * <tr>
 * <td>LTRIM(string)</td>
 * <td>Characters of string with leading blank spaces rh3oved</td>
 * </tr>
 * <tr>
 * <td>REPEAT(string, count)</td>
 * <td>A character string formed by repeating string count times</td>
 * </tr>
 * <tr>
 * <td>REPLACE(string1, string2, string3)</td>
 * <td>Replaces all occurrences of string2 in string1 with string3</td>
 * </tr>
 * <tr>
 * <td>RIGHT(string, count)</td>
 * <td>The count rightmost characters in string</td>
 * </tr>
 * <tr>
 * <td>RTRIM(string)</td>
 * <td>The characters of string with no trailing blanks</td>
 * </tr>
 * <tr>
 * <td>SOUNDEX(string)</td>
 * <td>A character string, which is data source-dependent, representing the sound of the words in string; this could be a four-digit SOUNDEX code, a phonetic representation of each word, etc.</td>
 * </tr>
 * <tr>
 * <td>SPACE(count)</td>
 * <td>A character string consisting of count spaces</td>
 * </tr>
 * <tr>
 * <td>SUBSTRING(string, start, length)</td>
 * <td>A character string formed by extracting length characters from string beginning at start</td>
 * </tr>
 * <tr>
 * <td>UCASE(string)</td>
 * <td>Converts all lowercase characters in string to uppercase</td>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h3>TIME and DATE FUNCTIONS</h3>
 * </td>
 * </tr>
 * <tr>
 * <td>CURDATE()</td>
 * <td>The current date as a date value</td>
 * </tr>
 * <tr>
 * <td>CURTIME()</td>
 * <td>The current local time as a time value</td>
 * </tr>
 * <tr>
 * <td>DAYNAME(date)</td>
 * <td>A character string representing the day component of date; the name for the day is specific to the data source</td>
 * </tr>
 * <tr>
 * <td>DAYOFMONTH(date)</td>
 * <td>An integer from 1 to 31 representing the day of the month in date</td>
 * </tr>
 * <tr>
 * <td>DAYOFWEEK(date)</td>
 * <td>An integer from 1 to 7 representing the day of the week in date; 1 represents Sunday</td>
 * </tr>
 * <tr>
 * <td>DAYOFYEAR(date)</td>
 * <td>An integer from 1 to 366 representing the day of the year in date</td>
 * </tr>
 * <tr>
 * <td>HOUR(time)</td>
 * <td>An integer from 0 to 23 representing the hour component of time</td>
 * </tr>
 * <tr>
 * <td>MINUTE(time)</td>
 * <td>An integer from 0 to 59 representing the minute component of time</td>
 * </tr>
 * <tr>
 * <td>MONTH(date)</td>
 * <td>An integer from 1 to 12 representing the month component of date</td>
 * </tr>
 * <tr>
 * <td>MONTHNAME(date)</td>
 * <td>A character string representing the month component of date; the name for the month is specific to the data source</td>
 * </tr>
 * <tr>
 * <td>NOW()</td>
 * <td>A timestamp value representing the current date and time</td>
 * </tr>
 * <tr>
 * <td>QUARTER(date)</td>
 * <td>An integer from 1 to 4 representing the quarter in date; 1 represents January 1 through March 31</td>
 * </tr>
 * <tr>
 * <td>SECOND(time)</td>
 * <td>An integer from 0 to 59 representing the second component of time</td>
 * </tr>
 * <tr>
 * <td>TIMESTAMPADD(interval,count, timestamp)</td>
 * <td>A timestamp calculated by adding count number of interval(s) to timestamp; interval may be one of the following: SQL_TSI_FRAC_SECOND, SQL_TSI_SECOND, SQL_TSI_MINUTE, SQL_TSI_HOUR, SQL_TSI_DAY, SQL_TSI_WEEK, SQL_TSI_MONTH, SQL_TSI_QUARTER, or SQL_TSI_YEAR</td>
 * </tr>
 * <tr>
 * <td>TIMESTAMPDIFF(interval,timestamp1, timestamp2)</td>
 * <td>An integer representing the number of interval(s) by which timestamp2 is greater than timestamp1; interval may be one of the following: SQL_TSI_FRAC_SECOND, SQL_TSI_SECOND, SQL_TSI_MINUTE, SQL_TSI_HOUR, SQL_TSI_DAY, SQL_TSI_WEEK, SQL_TSI_MONTH, SQL_TSI_QUARTER, or SQL_TSI_YEAR</td>
 * </tr>
 * <tr>
 * <td>WEEK(date)</td>
 * <td>An integer from 1 to 53 representing the week of the year in date</td>
 * </tr>
 * <tr>
 * <td>YEAR(date)</td>
 * <td>An integer representing the year component of date</td>
 * </tr>
 * <tr>
 * <td colspan="2"><br>
 *
 * <h3>SYSTEM FUNCTIONS</h3>
 * </td>
 * </tr>
 * <tr>
 * <td>DATABASE()</td>
 * <td>Name of the database</td>
 * </tr>
 * <tr>
 * <td>IFNULL(expression, value)</td>
 * <td>value if expression is null; expression if expression is not null</td>
 * </tr>
 * <tr>
 * <td>USER()</td>
 * <td>User name in the DBMS
 *
 * <tr>
 * <td colspan="2"><br>
 *
 * <h3>CONVERSION FUNCTIONS</h3>
 * </td>
 * </tr>
 * <tr>
 * <td>CONVERT(value, SQLtype)</td>
 * <td>value converted to SQLtype where SQLtype may be one of the following SQL types: BIGINT, BINARY, BIT, CHAR, DATE, DECIMAL, DOUBLE, FLOAT, INTEGER, LONGVARBINARY, LONGVARCHAR, REAL, SMALLINT, TIME, TIMESTAMP, TINYINT, VARBINARY, or VARCHAR</td>
 * </tr>
 * </table>
 */
public class SqlJdbcFunctionCall extends SqlFunction {

    /**
     * List of all numeric function names defined by JDBC.
     */
    private static final String NUMERIC_FUNCTIONS = constructFuncList( "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MOD", "PI", "POWER", "RADIANS", "RAND", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE" );

    /**
     * List of all string function names defined by JDBC.
     */
    private static final String STRING_FUNCTIONS = constructFuncList( "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "INSERT", "LCASE", "LEFT", "LENGTH", "LOCATE", "LTRIM", "REPEAT", "REPLACE", "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTRING", "UCASE" );
    // "ASCII", "CHAR", "DIFFERENCE", "LOWER",
    // "LEFT", "TRIM", "REPEAT", "REPLACE",
    // "RIGHT", "SPACE", "SUBSTRING", "UPPER", "INITCAP", "OVERLAY"

    /**
     * List of all time/date function names defined by JDBC.
     */
    private static final String TIME_DATE_FUNCTIONS = constructFuncList( "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW", "QUARTER", "SECOND", "TIMESTAMPADD", "TIMESTAMPDIFF", "WEEK", "YEAR" );

    /**
     * List of all system function names defined by JDBC.
     */
    private static final String SYSTEM_FUNCTIONS = constructFuncList( "CONVERT", "DATABASE", "IFNULL", "USER" );


    private final String jdbcName;
    private final MakeCall lookupMakeCallObj;
    private SqlCall lookupCall;

    private SqlNode[] operands;


    public SqlJdbcFunctionCall( String name ) {
        super(
                "{fn " + name + "}",
                Kind.JDBC_FN,
                null,
                null,
                OperandTypes.VARIADIC,
                FunctionCategory.SYSTEM );
        jdbcName = name;
        lookupMakeCallObj = JdbcToInternalLookupTable.INSTANCE.lookup( name );
        lookupCall = null;
    }


    private static String constructFuncList( String... functionNames ) {
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for ( String funcName : functionNames ) {
            if ( JdbcToInternalLookupTable.INSTANCE.lookup( funcName ) == null ) {
                continue;
            }
            if ( n++ > 0 ) {
                sb.append( "," );
            }
            sb.append( funcName );
        }
        return sb.toString();
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        this.operands = Arrays.stream( operands ).map( e -> (SqlNode) e ).toArray( SqlNode[]::new );
        return super.createCall( functionQualifier, pos, operands );
    }


    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        if ( null == lookupMakeCallObj ) {
            throw validator.newValidationError( call, Static.RESOURCE.functionUndefined( getName() ) );
        }
        return lookupMakeCallObj.getOperator().rewriteCall( validator, call );
    }


    public SqlCall getLookupCall() {
        if ( null == lookupCall ) {
            lookupCall = lookupMakeCallObj.createCall( ParserPos.ZERO, operands );
        }
        return lookupCall;
    }


    @Override
    public String getAllowedSignatures( String name ) {
        return lookupMakeCallObj.getOperator().getAllowedSignatures( name );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        // Override SqlFunction.deriveType, because function-resolution is not relevant to a JDBC function call.
        // REVIEW: jhyde: Should SqlJdbcFunctionCall even be a subclass of SqlFunction?

        for ( Node operand : call.getOperandList() ) {
            AlgDataType nodeType = validator.deriveType( scope, operand );
            ((SqlValidatorImpl) validator).setValidatedNodeType( (SqlNode) operand, nodeType );
        }
        return validateOperands( (SqlValidator) validator, (SqlValidatorScope) scope, (SqlCall) call );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        // only expected to come here if validator called this method
        SqlCallBinding callBinding = (SqlCallBinding) opBinding;

        if ( null == lookupMakeCallObj ) {
            throw callBinding.newValidationError( Static.RESOURCE.functionUndefined( getName() ) );
        }

        final String message = lookupMakeCallObj.isValidArgCount( callBinding );
        if ( message != null ) {
            throw callBinding.newValidationError( Static.RESOURCE.wrongNumberOfParam( getName(), operands.length, message ) );
        }

        final SqlCall newCall = getLookupCall();
        final SqlCallBinding newBinding = new SqlCallBinding( callBinding.getValidator(), callBinding.getScope(), newCall );

        final SqlOperator operator = lookupMakeCallObj.getOperator();
        if ( !operator.checkOperandTypes( newBinding, false ) ) {
            throw callBinding.newValidationSignatureError();
        }

        return operator.validateOperands( callBinding.getValidator(), callBinding.getScope(), newCall );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.print( "{fn " );
        writer.print( jdbcName );
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        for ( Node operand : call.getOperandList() ) {
            writer.sep( "," );
            ((SqlNode) operand).unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( frame );
        writer.print( "}" );
    }


    /**
     * @see java.sql.DatabaseMetaData#getNumericFunctions
     */
    public static String getNumericFunctions() {
        return NUMERIC_FUNCTIONS;
    }


    /**
     * @see java.sql.DatabaseMetaData#getStringFunctions
     */
    public static String getStringFunctions() {
        return STRING_FUNCTIONS;
    }


    /**
     * @see java.sql.DatabaseMetaData#getTimeDateFunctions
     */
    public static String getTimeDateFunctions() {
        return TIME_DATE_FUNCTIONS;
    }


    /**
     * @see java.sql.DatabaseMetaData#getSystemFunctions
     */
    public static String getSystemFunctions() {
        return SYSTEM_FUNCTIONS;
    }


    /**
     * Converts a call to a JDBC function to a call to a regular function.
     */
    private interface MakeCall {

        /**
         * Creates and return a {@link SqlCall}. If the MakeCall strategy object was created with a reordering specified the call will be created with the operands reordered, otherwise no change of ordering is applied
         *
         * @param operands Operands
         */
        SqlCall createCall( ParserPos pos, SqlNode... operands );

        SqlOperator getOperator();

        String isValidArgCount( SqlCallBinding binding );

    }


    /**
     * Converter that calls a built-in function with the same arguments.
     */
    public static class SimpleMakeCall implements SqlJdbcFunctionCall.MakeCall {

        final SqlOperator operator;


        public SimpleMakeCall( Operator operator ) {
            this.operator = (SqlOperator) operator;
        }


        @Override
        public SqlOperator getOperator() {
            return operator;
        }


        @Override
        public SqlCall createCall( ParserPos pos, SqlNode... operands ) {
            return (SqlCall) operator.createCall( pos, operands );
        }


        @Override
        public String isValidArgCount( SqlCallBinding binding ) {
            return null; // any number of arguments is valid
        }

    }


    /**
     * Implementation of {@link MakeCall} that can re-order or ignore operands.
     */
    private static class PermutingMakeCall extends SimpleMakeCall {

        final int[] order;


        /**
         * Creates a MakeCall strategy object with reordering of operands.
         *
         * The reordering is specified by an int array where the value of element at position <code>i</code> indicates to which element in a new SqlNode[] array the operand goes.
         *
         * @param operator Operator
         * @param order Order
         */
        PermutingMakeCall( Operator operator, int[] order ) {
            super( operator );
            this.order = Objects.requireNonNull( order );
        }


        @Override
        public SqlCall createCall( ParserPos pos, SqlNode... operands ) {
            return super.createCall( pos, reorder( operands ) );
        }


        @Override
        public String isValidArgCount( SqlCallBinding binding ) {
            if ( order.length == binding.getOperandCount() ) {
                return null; // operand count is valid
            } else {
                return getArgCountMismatchMsg( order.length );
            }
        }


        private String getArgCountMismatchMsg( int... possible ) {
            StringBuilder ret = new StringBuilder();
            for ( int i = 0; i < possible.length; i++ ) {
                if ( i > 0 ) {
                    ret.append( " or " );
                }
                ret.append( possible[i] );
            }
            ret.append( " parameter(s)" );
            return ret.toString();
        }


        /**
         * Uses the data in {@link #order} to reorder a SqlNode[] array.
         *
         * @param operands Operands
         */
        protected SqlNode[] reorder( SqlNode[] operands ) {
            assert operands.length == order.length;
            SqlNode[] newOrder = new SqlNode[operands.length];
            for ( int i = 0; i < operands.length; i++ ) {
                assert operands[i] != null;
                int joyDivision = order[i];
                assert newOrder[joyDivision] == null : "mapping is not 1:1";
                newOrder[joyDivision] = operands[i];
            }
            return newOrder;
        }

    }


    /**
     * Lookup table between JDBC functions and internal representation
     */
    private static class JdbcToInternalLookupTable {

        /**
         * The {@link Glossary#SINGLETON_PATTERN singleton} instance.
         */
        static final JdbcToInternalLookupTable INSTANCE = new JdbcToInternalLookupTable();

        private final Map<String, MakeCall> map;


        private JdbcToInternalLookupTable() {
            // A table of all functions can be found at http://java.sun.com/products/jdbc/driverdevs.html which is also provided in the javadoc for this class.
            // See also SqlOperatorTests.testJdbcFn, which contains the list.
            ImmutableMap.Builder<String, MakeCall> map = ImmutableMap.builder();
            map.put( "ABS", simple( OperatorRegistry.get( OperatorName.ABS ) ) );
            map.put( "ACOS", simple( OperatorRegistry.get( OperatorName.ACOS ) ) );
            map.put( "ASIN", simple( OperatorRegistry.get( OperatorName.ASIN ) ) );
            map.put( "ATAN", simple( OperatorRegistry.get( OperatorName.ATAN ) ) );
            map.put( "ATAN2", simple( OperatorRegistry.get( OperatorName.ATAN2 ) ) );
            map.put( "CEILING", simple( OperatorRegistry.get( OperatorName.CEIL ) ) );
            map.put( "COS", simple( OperatorRegistry.get( OperatorName.COS ) ) );
            map.put( "COT", simple( OperatorRegistry.get( OperatorName.COT ) ) );
            map.put( "DEGREES", simple( OperatorRegistry.get( OperatorName.DEGREES ) ) );
            map.put( "EXP", simple( OperatorRegistry.get( OperatorName.EXP ) ) );
            map.put( "FLOOR", simple( OperatorRegistry.get( OperatorName.FLOOR ) ) );
            map.put( "LOG", simple( OperatorRegistry.get( OperatorName.LN ) ) );
            map.put( "LOG10", simple( OperatorRegistry.get( OperatorName.LOG10 ) ) );
            map.put( "MOD", simple( OperatorRegistry.get( OperatorName.MOD ) ) );
            map.put( "PI", simple( OperatorRegistry.get( OperatorName.PI ) ) );
            map.put( "POWER", simple( OperatorRegistry.get( OperatorName.POWER ) ) );
            map.put( "RADIANS", simple( OperatorRegistry.get( OperatorName.RADIANS ) ) );
            map.put( "RAND", simple( OperatorRegistry.get( OperatorName.RAND ) ) );
            map.put( "ROUND", simple( OperatorRegistry.get( OperatorName.ROUND ) ) );
            map.put( "SIGN", simple( OperatorRegistry.get( OperatorName.SIGN ) ) );
            map.put( "SIN", simple( OperatorRegistry.get( OperatorName.SIN ) ) );
            map.put( "SQRT", simple( OperatorRegistry.get( OperatorName.SQRT ) ) );
            map.put( "TAN", simple( OperatorRegistry.get( OperatorName.TAN ) ) );
            map.put( "TRUNCATE", simple( OperatorRegistry.get( OperatorName.TRUNCATE ) ) );

            map.put( "CONCAT", simple( OperatorRegistry.get( OperatorName.CONCAT ) ) );
            map.put( "INSERT", new PermutingMakeCall( OperatorRegistry.get( OperatorName.OVERLAY ), new int[]{ 0, 2, 3, 1 } ) );
            map.put( "LCASE", simple( OperatorRegistry.get( OperatorName.LOWER ) ) );
            map.put( "LENGTH", simple( OperatorRegistry.get( OperatorName.CHARACTER_LENGTH ) ) );
            map.put( "LOCATE", simple( OperatorRegistry.get( OperatorName.POSITION ) ) );
            map.put(
                    "LTRIM",
                    new SimpleMakeCall( OperatorRegistry.get( OperatorName.TRIM ) ) {
                        @Override
                        public SqlCall createCall( ParserPos pos, SqlNode... operands ) {
                            assert 1 == operands.length;
                            return super.createCall(
                                    pos,
                                    SqlTrimFunction.Flag.LEADING.symbol( ParserPos.ZERO ),
                                    SqlLiteral.createCharString( " ", ParserPos.ZERO ),
                                    operands[0] );
                        }
                    } );
            map.put( "YEAR", simple( OperatorRegistry.get( OperatorName.YEAR ) ) );
            map.put( "QUARTER", simple( OperatorRegistry.get( OperatorName.QUARTER ) ) );
            map.put( "MONTH", simple( OperatorRegistry.get( OperatorName.MONTH ) ) );
            map.put( "WEEK", simple( OperatorRegistry.get( OperatorName.WEEK ) ) );
            map.put( "DAYOFYEAR", simple( OperatorRegistry.get( OperatorName.DAYOFYEAR ) ) );
            map.put( "DAYOFMONTH", simple( OperatorRegistry.get( OperatorName.DAYOFMONTH ) ) );
            map.put( "DAYOFWEEK", simple( OperatorRegistry.get( OperatorName.DAYOFWEEK ) ) );
            map.put( "HOUR", simple( OperatorRegistry.get( OperatorName.HOUR ) ) );
            map.put( "MINUTE", simple( OperatorRegistry.get( OperatorName.MINUTE ) ) );
            map.put( "SECOND", simple( OperatorRegistry.get( OperatorName.SECOND ) ) );

            map.put(
                    "RTRIM",
                    new SimpleMakeCall( OperatorRegistry.get( OperatorName.TRIM ) ) {
                        @Override
                        public SqlCall createCall( ParserPos pos, SqlNode... operands ) {
                            assert 1 == operands.length;
                            return super.createCall(
                                    pos,
                                    SqlTrimFunction.Flag.TRAILING.symbol( ParserPos.ZERO ),
                                    SqlLiteral.createCharString( " ", ParserPos.ZERO ),
                                    operands[0] );
                        }
                    } );
            map.put( "SUBSTRING", simple( OperatorRegistry.get( OperatorName.SUBSTRING ) ) );
            map.put( "REPLACE", simple( OperatorRegistry.get( OperatorName.REPLACE ) ) );
            map.put( "UCASE", simple( OperatorRegistry.get( OperatorName.UPPER ) ) );
            map.put( "CURDATE", simple( OperatorRegistry.get( OperatorName.CURRENT_DATE ) ) );
            map.put( "CURTIME", simple( OperatorRegistry.get( OperatorName.LOCALTIME ) ) );
            map.put( "NOW", simple( OperatorRegistry.get( OperatorName.CURRENT_TIMESTAMP ) ) );
            map.put( "TIMESTAMPADD", simple( OperatorRegistry.get( OperatorName.TIMESTAMP_ADD ) ) );
            map.put( "TIMESTAMPDIFF", simple( OperatorRegistry.get( OperatorName.TIMESTAMP_DIFF ) ) );

            map.put( "DATABASE", simple( OperatorRegistry.get( OperatorName.CURRENT_CATALOG ) ) );
            map.put(
                    "IFNULL",
                    new SimpleMakeCall( OperatorRegistry.get( OperatorName.COALESCE ) ) {
                        @Override
                        public SqlCall createCall( ParserPos pos, SqlNode... operands ) {
                            assert 2 == operands.length;
                            return super.createCall( pos, operands );
                        }
                    } );
            map.put( "USER", simple( OperatorRegistry.get( OperatorName.CURRENT_USER ) ) );
            map.put(
                    "CONVERT",
                    new SimpleMakeCall( OperatorRegistry.get( OperatorName.CAST ) ) {
                        @Override
                        public SqlCall createCall( ParserPos pos, SqlNode... operands ) {
                            assert 2 == operands.length;
                            SqlNode typeOperand = operands[1];
                            assert typeOperand.getKind() == Kind.LITERAL;

                            SqlJdbcDataTypeName jdbcType = ((SqlLiteral) typeOperand).symbolValue( SqlJdbcDataTypeName.class );

                            return super.createCall( pos, operands[0], jdbcType.createDataType( typeOperand.pos ) );
                        }
                    } );
            this.map = map.build();
        }


        private MakeCall simple( Operator operator ) {
            return new SimpleMakeCall( operator );
        }


        /**
         * Tries to lookup a given function name JDBC to an internal representation. Returns null if no function defined.
         */
        public MakeCall lookup( String name ) {
            return map.get( name );
        }

    }

}

