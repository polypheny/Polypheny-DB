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

package org.polypheny.db.sql.language;


import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Functions;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypePrecedenceList;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BasicNodeVisitor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.DynamicParam;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.schema.types.StreamableEntity;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.BarfingInvocationHandler;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Contains utility functions related to SQL parsing, all static.
 */
public abstract class SqlUtil {


    static SqlNode andExpressions( SqlNode node1, SqlNode node2 ) {
        if ( node1 == null ) {
            return node2;
        }
        List<Node> list = new ArrayList<>();
        if ( node1.getKind() == Kind.AND ) {
            list.addAll( ((SqlCall) node1).getOperandList() );
        } else {
            list.add( node1 );
        }
        if ( node2.getKind() == Kind.AND ) {
            list.addAll( ((SqlCall) node2).getOperandList() );
        } else {
            list.add( node2 );
        }
        return (SqlNode) OperatorRegistry.get( OperatorName.AND ).createCall( ParserPos.ZERO, list );
    }


    static List<SqlNode> flatten( SqlNode node ) {
        List<SqlNode> list = new ArrayList<>();
        flatten( node, list );
        return list;
    }


    /**
     * Returns the <code>n</code>th (0-based) input to a join expression.
     */
    public static SqlNode getFromNode( SqlSelect query, int ordinal ) {
        List<SqlNode> list = flatten( query.getSqlFrom() );
        return list.get( ordinal );
    }


    /**
     * Old method from commons-lang
     * it only turns single-quotes into doubled single-quotes ("McHale's Navy" => "McHale''s Navy")
     *
     * todo rewrite as it was removed
     * https://stackoverflow.com/questions/32096614/migrating-stringescapeutils-escapesql-from-commons-lang
     *
     * @param str input which is adjusted
     * @return the adjusted input
     */
    public static String escapeSql( String str ) {
        if ( str == null ) {
            return null;
        }
        return str.replace( "'", "''" );
    }


    private static void flatten( SqlNode node, List<SqlNode> list ) {
        switch ( node.getKind() ) {
            case JOIN:
                SqlJoin join = (SqlJoin) node;
                flatten( join.getLeft(), list );
                flatten( join.getRight(), list );
                return;
            case AS:
                SqlCall call = (SqlCall) node;
                flatten( call.operand( 0 ), list );
                return;
            default:
                list.add( node );
        }
    }


    /**
     * Converts an SqlNode array to a SqlNodeList
     */
    public static SqlNodeList toNodeList( SqlNode[] operands ) {
        SqlNodeList ret = new SqlNodeList( ParserPos.ZERO );
        for ( SqlNode node : operands ) {
            ret.add( node );
        }
        return ret;
    }


    /**
     * Returns whether a node represents the NULL value or a series of nested <code>CAST(NULL AS type)</code> calls. For example:
     * <code>isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1)))</code> returns {@code true}.
     */
    public static boolean isNull( SqlNode node ) {
        return CoreUtil.isNullLiteral( node, false )
                || node.getKind() == Kind.CAST
                && isNull( ((SqlCall) node).operand( 0 ) );
    }


    /**
     * Returns whether a node is a literal.
     *
     * Examples:
     *
     * <ul>
     * <li>For <code>CAST(literal AS <i>type</i>)</code>, returns true if <code>allowCast</code> is true, false otherwise.</li>
     * <li>For <code>CAST(CAST(literal AS <i>type</i>) AS <i>type</i>))</code>, returns false.</li>
     * </ul>
     *
     * @param node The node, never null.
     * @param allowCast whether to regard CAST(literal) as a literal
     * @return Whether the node is a literal
     */
    public static boolean isLiteral( SqlNode node, boolean allowCast ) {
        assert node != null;
        if ( node instanceof SqlLiteral ) {
            return true;
        }
        if ( allowCast ) {
            if ( node.getKind() == Kind.CAST ) {
                SqlCall call = (SqlCall) node;
                if ( isLiteral( call.operand( 0 ), false ) ) {
                    // node is "CAST(literal as type)"
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Returns whether a node is a literal.
     *
     * Many constructs which require literals also accept <code>CAST(NULL AS <i>type</i>)</code>. This method does not accept casts, so you should call {@link CoreUtil#isNullLiteral} first.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal
     */
    public static boolean isLiteral( SqlNode node ) {
        return isLiteral( node, false );
    }


    /**
     * Returns whether a node is a literal chain which is used to represent a continued string literal.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal chain
     */
    public static boolean isLiteralChain( SqlNode node ) {
        assert node != null;
        if ( node instanceof SqlCall ) {
            SqlCall call = (SqlCall) node;
            return call.getKind() == Kind.LITERAL_CHAIN;
        } else {
            return false;
        }
    }


    /**
     * Unparses a call to an operator which has function syntax.
     *
     * @param operator The operator
     * @param writer Writer
     * @param call List of 0 or more operands
     */
    public static void unparseFunctionSyntax( SqlOperator operator, SqlWriter writer, SqlCall call ) {
        if ( operator instanceof SqlFunction ) {
            SqlFunction function = (SqlFunction) operator;

            if ( function.getFunctionCategory().isSpecific() ) {
                writer.keyword( "SPECIFIC" );
            }
            SqlIdentifier id = function.getSqlIdentifier();
            if ( id == null ) {
                writer.keyword( operator.getName() );
            } else {
                id.unparse( writer, 0, 0 );
            }
        } else {
            writer.print( operator.getName() );
        }
        if ( call.operandCount() == 0 ) {
            switch ( call.getOperator().getSyntax() ) {
                case FUNCTION_ID:
                    // For example, the "LOCALTIME" function appears as "LOCALTIME" when it has 0 args, not "LOCALTIME()".
                    return;
                case FUNCTION_STAR: // E.g. "COUNT(*)"
                case FUNCTION: // E.g. "RANK()"
                    // fall through - dealt with below
            }
        }
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")" );
        final SqlLiteral quantifier = call.getFunctionQuantifier();
        if ( quantifier != null ) {
            quantifier.unparse( writer, 0, 0 );
        }
        if ( call.operandCount() == 0 ) {
            if ( Objects.requireNonNull( call.getOperator().getSyntax() ) == Syntax.FUNCTION_STAR ) {
                writer.sep( "*" );
            }
        }
        for ( Node operand : call.getOperandList() ) {
            writer.sep( "," );
            ((SqlNode) operand).unparse( writer, 0, 0 );
        }
        writer.endList( frame );
    }


    public static void unparseBinarySyntax( SqlOperator operator, SqlCall call, SqlWriter writer, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame =
                writer.startList(
                        (operator instanceof SqlSetOperator)
                                ? SqlWriter.FrameTypeEnum.SETOP
                                : SqlWriter.FrameTypeEnum.SIMPLE );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, operator.getLeftPrec() );
        final boolean needsSpace = operator.needsSpace();
        writer.setNeedWhitespace( needsSpace );
        writer.sep( operator.getName() );
        writer.setNeedWhitespace( needsSpace );
        ((SqlNode) call.operand( 1 )).unparse( writer, operator.getRightPrec(), rightPrec );
        writer.endList( frame );
    }


    /**
     * Concatenates string literals.
     *
     * This method takes an array of arguments, since pairwise concatenation means too much string copying.
     *
     * @param lits an array of {@link SqlLiteral}, not empty, all of the same class
     * @return a new {@link SqlLiteral}, of that same class, whose value is the string concatenation of the values of the literals
     * @throws ClassCastException if the lits are not homogeneous.
     * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
     */
    public static SqlLiteral concatenateLiterals( List<SqlLiteral> lits ) {
        if ( lits.size() == 1 ) {
            return lits.get( 0 ); // nothing to do
        }
        return ((SqlAbstractStringLiteral) lits.get( 0 )).concat1( lits );
    }


    /**
     * Looks up a (possibly overloaded) routine based on name and argument types.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param argNames argument names, or null if call by position
     * @param category whether a function or a procedure. (If a procedure is being invoked, the overload rules are simpler.)
     * @return matching routine, or null if none found
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4
     */
    public static SqlOperator lookupRoutine( OperatorTable opTab, SqlIdentifier funcName, List<AlgDataType> argTypes, List<String> argNames, FunctionCategory category, SqlSyntax syntax, Kind Kind ) {
        Iterator<SqlOperator> list =
                lookupSubjectRoutines(
                        opTab,
                        funcName,
                        argTypes,
                        argNames,
                        syntax,
                        Kind,
                        category );
        if ( list.hasNext() ) {
            // return first on schema path
            return list.next();
        }
        return null;
    }


    private static Iterator<SqlOperator> filterOperatorRoutinesByKind( Iterator<SqlOperator> routines, final Kind Kind ) {
        return Iterators.filter( routines, operator -> Objects.requireNonNull( operator ).getKind() == Kind );
    }


    /**
     * Looks up all subject routines matching the given name and argument types.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param argNames argument names, or null if call by position
     * @param sqlSyntax the SqlSyntax of the SqlOperator being looked up
     * @param Kind the Kind of the SqlOperator being looked up
     * @param category category of routine to look up
     * @return list of matching routines
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4
     */
    public static Iterator<SqlOperator> lookupSubjectRoutines( OperatorTable opTab, SqlIdentifier funcName, List<AlgDataType> argTypes, List<String> argNames, SqlSyntax sqlSyntax, Kind Kind, FunctionCategory category ) {
        // start with all routines matching by name
        Iterator<SqlOperator> routines = lookupSubjectRoutinesByName( opTab, funcName, sqlSyntax, category );

        // first pass:  eliminate routines which don't accept the given number of arguments
        routines = filterRoutinesByParameterCount( routines, argTypes );

        // NOTE: according to SQL99, procedures are NOT overloaded on type, only on number of arguments.
        if ( category == FunctionCategory.USER_DEFINED_PROCEDURE ) {
            return routines;
        }

        // second pass:  eliminate routines which don't accept the given argument types
        routines = filterRoutinesByParameterType( sqlSyntax, routines, argTypes, argNames );

        // see if we can stop now; this is necessary for the case of builtin functions where we don't have param type info
        final List<SqlOperator> list = Lists.newArrayList( routines );
        routines = list.iterator();
        if ( list.size() < 2 ) {
            return routines;
        }

        // third pass:  for each parameter from left to right, eliminate all routines except those with the best precedence match for the given arguments
        routines = filterRoutinesByTypePrecedence( sqlSyntax, routines, argTypes );

        // fourth pass: eliminate routines which do not have the same Kind as requested
        return filterOperatorRoutinesByKind( routines, Kind );
    }


    /**
     * Determines whether there is a routine matching the given name and number of arguments.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param category category of routine to look up
     * @return true if match found
     */
    public static boolean matchRoutinesByParameterCount( OperatorTable opTab, SqlIdentifier funcName, List<AlgDataType> argTypes, FunctionCategory category ) {
        // start with all routines matching by name
        Iterator<SqlOperator> routines = lookupSubjectRoutinesByName( opTab, funcName, SqlSyntax.FUNCTION, category );

        // first pass:  eliminate routines which don't accept the given number of arguments
        routines = filterRoutinesByParameterCount( routines, argTypes );

        return routines.hasNext();
    }


    private static Iterator<SqlOperator> lookupSubjectRoutinesByName( OperatorTable opTab, SqlIdentifier funcName, final SqlSyntax syntax, FunctionCategory category ) {
        final List<Operator> operators = new ArrayList<>();
        opTab.lookupOperatorOverloads( funcName, category, syntax.getSyntax(), operators );
        final List<SqlOperator> sqlOperators = operators.stream().map( e -> (SqlOperator) e ).toList();
        if ( syntax == SqlSyntax.FUNCTION ) {
            return Iterators.filter( sqlOperators.iterator(), Predicates.instanceOf( SqlFunction.class ) );
        }
        return Iterators.filter( sqlOperators.iterator(), operator -> Objects.requireNonNull( operator ).getSqlSyntax() == syntax );
    }


    private static Iterator<SqlOperator> filterRoutinesByParameterCount( Iterator<SqlOperator> routines, final List<AlgDataType> argTypes ) {
        return Iterators.filter(
                routines,
                operator -> Objects.requireNonNull( operator ).getOperandCountRange().isValidCount( argTypes.size() ) );
    }


    /**
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4 Syntax Rule 6.b.iii.2.B
     */
    private static Iterator<SqlOperator> filterRoutinesByParameterType( SqlSyntax syntax, final Iterator<SqlOperator> routines, final List<AlgDataType> argTypes, final List<String> argNames ) {
        if ( syntax != SqlSyntax.FUNCTION ) {
            return routines;
        }

        //noinspection unchecked
        return (Iterator) Iterators.filter(
                Iterators.filter( routines, SqlFunction.class ),
                function -> {
                    List<AlgDataType> paramTypes = Objects.requireNonNull( function ).getParamTypes();
                    if ( paramTypes == null ) {
                        // no parameter information for builtins; keep for now
                        return true;
                    }
                    final List<AlgDataType> permutedArgTypes;
                    if ( argNames != null ) {
                        // Arguments passed by name. Make sure that the function has parameters of all of these names.
                        final Map<Integer, Integer> map = new HashMap<>();
                        for ( Ord<String> argName : Ord.zip( argNames ) ) {
                            final int i = function.getParamNames().indexOf( argName.e );
                            if ( i < 0 ) {
                                return false;
                            }
                            map.put( i, argName.i );
                        }
                        permutedArgTypes = Functions.generate( paramTypes.size(), a0 -> {
                            if ( map.containsKey( a0 ) ) {
                                return argTypes.get( map.get( a0 ) );
                            } else {
                                return null;
                            }
                        } );
                    } else {
                        permutedArgTypes = Lists.newArrayList( argTypes );
                        while ( permutedArgTypes.size() < argTypes.size() ) {
                            paramTypes.add( null );
                        }
                    }
                    for ( Pair<AlgDataType, AlgDataType> p : Pair.zip( paramTypes, permutedArgTypes ) ) {
                        final AlgDataType argType = p.right;
                        final AlgDataType paramType = p.left;
                        if ( argType != null && !PolyTypeUtil.canCastFrom( paramType, argType, false ) ) {
                            return false;
                        }
                    }
                    return true;
                } );
    }


    /**
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 9.4
     */
    private static Iterator<SqlOperator> filterRoutinesByTypePrecedence( SqlSyntax sqlSyntax, Iterator<SqlOperator> routines, List<AlgDataType> argTypes ) {
        if ( sqlSyntax != SqlSyntax.FUNCTION ) {
            return routines;
        }

        List<SqlFunction> sqlFunctions =
                Lists.newArrayList( Iterators.filter( routines, SqlFunction.class ) );

        for ( final Ord<AlgDataType> argType : Ord.zip( argTypes ) ) {
            final AlgDataTypePrecedenceList precList = argType.e.getPrecedenceList();
            final AlgDataType bestMatch = bestMatch( sqlFunctions, argType.i, precList );
            if ( bestMatch != null ) {
                sqlFunctions = sqlFunctions.stream()
                        .filter( function -> {
                            final List<AlgDataType> paramTypes = function.getParamTypes();
                            if ( paramTypes == null ) {
                                return false;
                            }
                            final AlgDataType paramType = paramTypes.get( argType.i );
                            return precList.compareTypePrecedence( paramType, bestMatch ) >= 0;
                        } )
                        .toList();
            }
        }
        //noinspection unchecked
        return (Iterator) sqlFunctions.iterator();
    }


    private static AlgDataType bestMatch( List<SqlFunction> sqlFunctions, int i, AlgDataTypePrecedenceList precList ) {
        AlgDataType bestMatch = null;
        for ( SqlFunction function : sqlFunctions ) {
            List<AlgDataType> paramTypes = function.getParamTypes();
            if ( paramTypes == null ) {
                continue;
            }
            final AlgDataType paramType = paramTypes.get( i );
            if ( bestMatch == null ) {
                bestMatch = paramType;
            } else {
                int c = precList.compareTypePrecedence( bestMatch, paramType );
                if ( c < 0 ) {
                    bestMatch = paramType;
                }
            }
        }
        return bestMatch;
    }


    /**
     * If an identifier is a legitimate call to a function which has no arguments and requires no parentheses (for example "CURRENT_USER"), returns a call to that function, otherwise returns null.
     */
    public static SqlCall makeCall( OperatorTable opTab, SqlIdentifier id ) {
        if ( id.names.size() == 1 ) {
            final List<Operator> list = new ArrayList<>();
            opTab.lookupOperatorOverloads( id, null, Syntax.FUNCTION, list );
            for ( Operator operator : list ) {
                if ( operator.getSyntax() == Syntax.FUNCTION_ID ) {
                    // Even though this looks like an identifier, it is a actually a call to a function. Construct a fake call to this function, so we can use the regular operator validation.
                    return new SqlBasicCall(
                            (SqlOperator) operator,
                            SqlNode.EMPTY_ARRAY,
                            id.getPos(),
                            true,
                            null );
                }
            }
        }
        return null;
    }


    /**
     * Returns whether a {@link SqlNode node} is a {@link SqlCall call} to a given {@link SqlOperator operator}.
     */
    public static boolean isCallTo( SqlNode node, Operator operator ) {
        return (node instanceof SqlCall) && (((SqlCall) node).getOperator().equals( operator ));
    }


    /**
     * If a node is "AS", returns the underlying expression; otherwise returns the node.
     */
    public static SqlNode stripAs( SqlNode node ) {
        if ( node != null && node.getKind() == Kind.AS ) {
            return ((SqlCall) node).operand( 0 );
        }
        return node;
    }


    /**
     * Returns a list of ancestors of {@code predicate} within a given {@code SqlNode} tree.
     *
     * The first element of the list is {@code root}, and the last is the node that matched {@code predicate}. Throws if no node matches.
     */
    public static ImmutableList<SqlNode> getAncestry( SqlNode root, Predicate<SqlNode> predicate, Predicate<SqlNode> postPredicate ) {
        try {
            new Genealogist( predicate, postPredicate ).visitChild( root );
            throw new AssertionError( "not found: " + predicate + " in " + root );
        } catch ( Util.FoundOne e ) {
            //noinspection unchecked
            return (ImmutableList<SqlNode>) e.getNode();
        }
    }


    public static List<List<Node>> toNodeListList( List<List<SqlNode>> sqlList ) {
        return sqlList.stream().map( CoreUtil::toNodeList ).toList();
    }


    public static SqlLiteral symbol( Enum<?> o, ParserPos parserPos ) {
        return SqlLiteral.createSymbol( o, parserPos );
    }


    public static AlgDataType getNamedType( Identifier node, Snapshot snapshot ) {
        LogicalTable table = snapshot.rel().getTable( node.getNames().get( 0 ), node.getNames().get( 1 ) ).orElse( null );
        if ( table != null ) {
            return table.getTupleType();
        } else {
            return null;
        }
    }


    public static boolean supportsModality( Modality modality, Entity entity ) {

        if ( Objects.requireNonNull( modality ) == Modality.STREAM ) {
            return entity instanceof StreamableEntity;
        }
        return !(entity instanceof StreamableEntity);

    }


    /**
     * Handles particular {@link DatabaseMetaData} methods; invocations of other methods will fall through to the base class,
     * {@link org.polypheny.db.util.BarfingInvocationHandler}, which will throw an error.
     */
    public static class DatabaseMetaDataInvocationHandler extends BarfingInvocationHandler {

        private final String databaseProductName;
        private final String identifierQuoteString;


        public DatabaseMetaDataInvocationHandler( String databaseProductName, String identifierQuoteString ) {
            this.databaseProductName = databaseProductName;
            this.identifierQuoteString = identifierQuoteString;
        }


        public String getDatabaseProductName() throws SQLException {
            return databaseProductName;
        }


        public String getIdentifierQuoteString() throws SQLException {
            return identifierQuoteString;
        }

    }


    /**
     * Walks over a {@link SqlNode} tree and returns the ancestry stack when it finds a given node.
     */
    private static class Genealogist extends BasicNodeVisitor<Void> {

        private final List<SqlNode> ancestors = new ArrayList<>();
        private final Predicate<SqlNode> predicate;
        private final Predicate<SqlNode> postPredicate;


        Genealogist( Predicate<SqlNode> predicate, Predicate<SqlNode> postPredicate ) {
            this.predicate = predicate;
            this.postPredicate = postPredicate;
        }


        private Void check( SqlNode node ) {
            preCheck( node );
            postCheck( node );
            return null;
        }


        private Void preCheck( SqlNode node ) {
            if ( predicate.test( node ) ) {
                throw new Util.FoundOne( ImmutableList.copyOf( ancestors ) );
            }
            return null;
        }


        private Void postCheck( SqlNode node ) {
            if ( postPredicate.test( node ) ) {
                throw new Util.FoundOne( ImmutableList.copyOf( ancestors ) );
            }
            return null;
        }


        private void visitChild( SqlNode node ) {
            if ( node == null ) {
                return;
            }
            ancestors.add( node );
            node.accept( this );
            ancestors.remove( ancestors.size() - 1 );
        }


        @Override
        public Void visit( Identifier id ) {
            return check( (SqlNode) id );
        }


        @Override
        public Void visit( Call call ) {
            preCheck( (SqlNode) call );
            for ( Node node : call.getOperandList() ) {
                visitChild( (SqlNode) node );
            }
            return postCheck( (SqlNode) call );
        }


        @Override
        public Void visit( IntervalQualifier intervalQualifier ) {
            return check( (SqlNode) intervalQualifier );
        }


        @Override
        public Void visit( Literal literal ) {
            return check( (SqlNode) literal );
        }


        @Override
        public Void visit( NodeList nodeList ) {
            preCheck( (SqlNode) nodeList );
            for ( Node node : nodeList ) {
                visitChild( (SqlNode) node );
            }
            return postCheck( (SqlNode) nodeList );
        }


        @Override
        public Void visit( DynamicParam param ) {
            return check( (SqlNode) param );
        }


        @Override
        public Void visit( DataTypeSpec type ) {
            return check( (SqlNode) type );
        }

    }


    static public List<List<SqlNode>> toSqlListList( List<List<? extends Node>> nodes ) {
        return nodes.stream().map( SqlUtil::toSqlList ).toList();
    }


    static public <T extends Node> List<List<? extends Node>> toSqlListList( List<List<? extends Node>> nodes, Class<? extends Node> clazz ) {
        return nodes.stream().map( e -> toSqlList( e, clazz ) ).collect( Collectors.toList() );
    }


    static public List<SqlNode> toSqlList( List<? extends Node> nodes ) {
        return toSqlList( nodes, SqlNode.class );
    }


    static public <T extends Node> List<T> toSqlList( List<? extends Node> nodes, Class<T> clazz ) {
        return nodes.stream().map( clazz::cast ).toList();
    }


    /*
     * Splits a string with comments and multiple statements into multiple strings with one statement each and without comments.
     * This is separate until the parser can deal with this.
     */
    public static List<String> splitStatements( String statements ) {
        List<String> split = new ArrayList<>();
        Stack<Character> brackets = new Stack<>();
        StringBuilder currentStatement = new StringBuilder();
        Character quote = null;

        for ( int i = 0; i < statements.length(); i++ ) {
            char ch = statements.charAt( i );

            if ( quote != null && ch == quote ) {
                if ( i + 1 == statements.length() || statements.charAt( i + 1 ) != quote ) {
                    quote = null;
                } else {
                    currentStatement.append( quote );
                    currentStatement.append( quote );
                    i += 1;
                    continue;
                }
            } else if ( quote == null ) {
                if ( ch == '\'' || ch == '"' ) {
                    quote = ch;
                } else if ( ch == '(' || ch == '[' || ch == '{' ) {
                    brackets.push( ch == '(' ? ')' : ch == '[' ? ']' : '}' );
                } else if ( ch == ')' || ch == ']' || ch == '}' ) {
                    if ( ch != brackets.pop() ) {
                        throw new GenericRuntimeException( "Unbalanced brackets" );
                    }
                } else if ( ch == ';' ) {
                    if ( !brackets.isEmpty() ) {
                        throw new GenericRuntimeException( "Missing " + brackets.pop() );
                    }
                    split.add( currentStatement.toString() );
                    currentStatement = new StringBuilder();
                    continue;
                } else if ( ch == '-' && i + 1 < statements.length() && statements.charAt( i + 1 ) == '-' ) {
                    i += 1;
                    while ( i + 1 < statements.length() && statements.charAt( i + 1 ) != '\n' ) {
                        i++;
                    }
                    // i + 1 < statements.length() means that statements.charAt( i + 1 ) == '\n'
                    if ( i + 1 < statements.length() ) {
                        i++;
                    }
                    // This whitespace prevents constructions like "SEL--\nECT" from resulting in valid SQL
                    ch = ' ';
                } else if ( ch == '/' && i + 1 < statements.length() && statements.charAt( i + 1 ) == '*' ) {
                    i += 2;
                    while ( i + 1 < statements.length() && !(statements.charAt( i ) == '*' && statements.charAt( i + 1 ) == '/') ) {
                        i++;
                    }
                    if ( i + 1 == statements.length() ) {
                        throw new GenericRuntimeException( "Unterminated comment" );
                    }
                    i++;
                    // Same reason as above for cases like "SEL/**/ECT"
                    ch = ' ';
                }
            }
            currentStatement.append( ch );
        }

        if ( quote != null ) {
            throw new GenericRuntimeException( String.format( "Unterminated %s", quote ) );
        }

        if ( !brackets.empty() ) {
            throw new GenericRuntimeException( "Missing " + brackets.pop() );
        }

        if ( !currentStatement.toString().isBlank() ) {
            split.add( currentStatement.toString() );
        }

        return split.stream().map( String::strip ).toList();
    }

}

