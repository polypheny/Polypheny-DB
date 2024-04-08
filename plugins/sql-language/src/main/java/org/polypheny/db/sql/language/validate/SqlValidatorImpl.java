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

package org.polypheny.db.sql.language.validate;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.polypheny.db.algebra.constant.AccessEnum;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.JoinConditionType;
import org.polypheny.db.algebra.constant.JoinType;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.MonikerType;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.DynamicRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.BasicNodeVisitor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.DynamicParam;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.validate.ValidatorException;
import org.polypheny.db.nodes.validate.ValidatorNamespace;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.runtime.Feature;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources;
import org.polypheny.db.runtime.Resources.ExInst;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.document.DocumentUtil;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlCharStringLiteral;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDelete;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlExplain;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlInsert;
import org.polypheny.db.sql.language.SqlIntervalLiteral;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlJoin;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.sql.language.SqlMerge;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlOrderBy;
import org.polypheny.db.sql.language.SqlSampleSpec;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlSelectKeyword;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlUnresolvedFunction;
import org.polypheny.db.sql.language.SqlUpdate;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.sql.language.SqlWith;
import org.polypheny.db.sql.language.SqlWithItem;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.sql.language.fun.SqlCrossMapItemOperator;
import org.polypheny.db.sql.language.util.SqlShuttle;
import org.polypheny.db.sql.language.util.SqlTypeUtil;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.AssignableOperandTypeChecker;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.AccessType;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.MonikerImpl;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Default implementation of {@link SqlValidator}.
 */
public class SqlValidatorImpl implements SqlValidatorWithHints {

    public static final Logger TRACER = PolyphenyDbTrace.PARSER_LOGGER;

    /**
     * Alias generated for the source table when rewriting UPDATE to MERGE.
     */
    public static final String UPDATE_SRC_ALIAS = "SYS$SRC";

    /**
     * Alias generated for the target table when rewriting UPDATE to MERGE if no alias was specified by the user.
     */
    public static final String UPDATE_TGT_ALIAS = "SYS$TGT";

    /**
     * Alias prefix generated for source columns when rewriting UPDATE to MERGE.
     */
    public static final String UPDATE_ANON_PREFIX = "SYS$ANON";


    private final OperatorTable opTab;

    @Getter
    final Snapshot snapshot;

    /**
     * Maps ParsePosition strings to the {@link SqlIdentifier} identifier objects at these positions
     */
    protected final Map<String, IdInfo> idPositions = new HashMap<>();

    /**
     * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope} scope created from them.
     */
    protected final Map<SqlNode, SqlValidatorScope> scopes = new IdentityHashMap<>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING clauses.
     */
    private final Map<SqlSelect, SqlValidatorScope> whereScopes = new IdentityHashMap<>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its GROUP BY clause.
     */
    private final Map<SqlSelect, SqlValidatorScope> groupByScopes = new IdentityHashMap<>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING clauses.
     */
    private final Map<SqlSelect, SqlValidatorScope> selectScopes = new IdentityHashMap<>();

    /**
     * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
     */
    private final Map<SqlSelect, SqlValidatorScope> orderScopes = new IdentityHashMap<>();

    /**
     * Maps a {@link SqlSelect} node that is the argument to a CURSOR constructor to the scope of the result of that select node
     */
    private final Map<SqlSelect, SqlValidatorScope> cursorScopes = new IdentityHashMap<>();

    /**
     * The name-resolution scope of a LATERAL TABLE clause.
     */
    @Getter
    private TableScope tableScope = null;

    /**
     * Maps a {@link SqlNode node} to the {@link SqlValidatorNamespace namespace} which describes what columns they contain.
     */
    protected final Map<SqlNode, SqlValidatorNamespace> namespaces = new IdentityHashMap<>();

    /**
     * Set of select expressions used as cursor definitions. In standard SQL, only the top-level SELECT is a cursor; Polypheny-DB extends this with cursors as inputs to table functions.
     */
    private final Set<SqlNode> cursorSet = Sets.newIdentityHashSet();

    /**
     * Stack of objects that maintain information about function calls. A stack is needed to handle nested function calls. The function call currently being validated is at the top of the stack.
     */
    protected final Deque<FunctionParamInfo> functionCallStack = new ArrayDeque<>();

    private int nextGeneratedId;

    @Getter
    protected final AlgDataTypeFactory typeFactory;

    /**
     * The type of dynamic parameters until a type is imposed on them.
     */
    @Getter
    protected final AlgDataType unknownType;
    private final AlgDataType booleanType;

    /**
     * Map of derived {@link AlgDataType} for each node. This is an IdentityHashMap since in some cases (such as null literals) we need to discriminate by instance.
     */
    private final Map<SqlNode, AlgDataType> nodeToTypeMap = new IdentityHashMap<>();
    private final AggFinder aggFinder;
    private final AggFinder aggOrOverFinder;
    private final AggFinder aggOrOverOrGroupFinder;
    private final AggFinder groupFinder;
    private final AggFinder overFinder;
    @Getter
    private final Conformance conformance;
    private final Map<SqlNode, SqlNode> originalExprs = new HashMap<>();

    private SqlNode top;

    // REVIEW jvs: subclasses may override shouldExpandIdentifiers in a way that ignores this; we should probably get rid of the protected method and always use this variable (or better, move preferences like
    // this to a separate "parameter" class)
    protected boolean expandIdentifiers;

    protected boolean expandColumnReferences;

    private boolean rewriteCalls;

    private NullCollation nullCollation = NullCollation.HIGH;

    // TODO jvs: make this local to performUnconditionalRewrites if it's OK to expand the signature of that method.
    private boolean validatingSqlMerge;

    private boolean inWindow; // Allow nested aggregates

    @Getter
    private final SqlValidatorImpl.ValidationErrorFunction validationErrorFunction = new SqlValidatorImpl.ValidationErrorFunction();


    /**
     * Creates a validator.
     *
     * @param opTab Operator table
     * @param typeFactory Type factory
     * @param conformance Compatibility mode
     */
    public SqlValidatorImpl( OperatorTable opTab, Snapshot snapshot, AlgDataTypeFactory typeFactory, Conformance conformance ) {
        this.opTab = Objects.requireNonNull( opTab );
        this.snapshot = Objects.requireNonNull( snapshot );
        this.typeFactory = Objects.requireNonNull( typeFactory );
        this.conformance = Objects.requireNonNull( conformance );

        unknownType = typeFactory.createUnknownType();
        booleanType = typeFactory.createPolyType( PolyType.BOOLEAN );

        rewriteCalls = true;
        expandColumnReferences = true;
        aggFinder = new AggFinder( opTab, false, true, false, null );
        aggOrOverFinder = new AggFinder( opTab, true, true, false, null );
        overFinder = new AggFinder( opTab, true, false, false, aggOrOverFinder );
        groupFinder = new AggFinder( opTab, false, false, true, null );
        aggOrOverOrGroupFinder = new AggFinder( opTab, true, true, true, null );
    }


    @Override
    public OperatorTable getOperatorTable() {
        return opTab;
    }


    @Override
    public Node validate( Node node ) {
        if ( node.getLanguage() != QueryLanguage.from( "sql" ) ) {
            throw new GenericRuntimeException( "Non-SQL queries cannot be evaluated with an SQL validator" );
        }

        return validateSql( (SqlNode) node );
    }


    @Override
    public SqlNodeList expandStar( SqlNodeList selectList, SqlSelect select, boolean includeSystemVars ) {
        final List<SqlNode> list = new ArrayList<>();
        final List<AlgDataTypeField> types = new ArrayList<>();
        for ( int i = 0; i < selectList.size(); i++ ) {
            final SqlNode selectItem = (SqlNode) selectList.get( i );
            final AlgDataType originalType = getValidatedNodeTypeIfKnown( selectItem );
            expandSelectItem(
                    selectItem,
                    select,
                    Util.first( originalType, unknownType ),
                    list,
                    NameMatchers.withCaseSensitive( false ).createSet(),
                    types,
                    includeSystemVars );
        }
        getRawSelectScope( select ).setExpandedSelectList( list );
        return new SqlNodeList( list, ParserPos.ZERO );
    }


    // implement SqlValidator
    @Override
    public void declareCursor( SqlSelect select, SqlValidatorScope parentScope ) {
        cursorSet.add( select );

        // add the cursor to a map that maps the cursor to its select based on the position of the cursor relative to other cursors in that call
        FunctionParamInfo funcParamInfo = functionCallStack.peek();
        Map<Integer, SqlSelect> cursorMap = funcParamInfo.cursorPosToSelectMap;
        int numCursors = cursorMap.size();
        cursorMap.put( numCursors, select );

        // create a namespace associated with the result of the select that is the argument to the cursor constructor; register it with a scope corresponding to the cursor
        SelectScope cursorScope = new SelectScope( parentScope, null, select );
        cursorScopes.put( select, cursorScope );
        final SelectNamespace selectNs = createSelectNamespace( select, select );
        String alias = deriveAlias( select, nextGeneratedId++ );
        registerNamespace( cursorScope, alias, selectNs, false );
    }


    // implement SqlValidator
    @Override
    public void pushFunctionCall() {
        FunctionParamInfo funcInfo = new FunctionParamInfo();
        functionCallStack.push( funcInfo );
    }


    // implement SqlValidator
    @Override
    public void popFunctionCall() {
        functionCallStack.pop();
    }


    // implement SqlValidator
    @Override
    public String getParentCursor( String columnListParamName ) {
        FunctionParamInfo funcParamInfo = functionCallStack.peek();
        Map<String, String> parentCursorMap = funcParamInfo.columnListParamToParentCursorMap;
        return parentCursorMap.get( columnListParamName );
    }


    /**
     * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns true; otherwise writes the unexpanded item.
     *
     * @param selectItem Select-list item
     * @param select Containing select clause
     * @param selectItems List that expanded items are written to
     * @param aliases Set of aliases
     * @param fields List of field names and types, in alias order
     * @param includeSystemVars If true include system vars in lists
     * @return Whether the node was expanded
     */
    private boolean expandSelectItem( final SqlNode selectItem, SqlSelect select, AlgDataType targetType, List<SqlNode> selectItems, Set<String> aliases, List<AlgDataTypeField> fields, final boolean includeSystemVars ) {
        final SelectScope scope = (SelectScope) getWhereScope( select );
        if ( expandStar( selectItems, aliases, fields, includeSystemVars, scope, selectItem ) ) {
            return true;
        }

        // Expand the select item: fully-qualify columns, and convert parentheses-free functions such as LOCALTIME into explicit function calls.
        SqlNode expanded = expand( selectItem, scope );
        final String alias = deriveAlias( selectItem, aliases.size() );

        // If expansion has altered the natural alias, supply an explicit 'AS'.
        final SqlValidatorScope selectScope = getSelectScope( select );
        if ( expanded != selectItem ) {
            String newAlias = deriveAlias( expanded, aliases.size() );
            if ( !newAlias.equals( alias ) ) {
                expanded =
                        (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall(
                                selectItem.getPos(),
                                expanded,
                                new SqlIdentifier( alias, ParserPos.ZERO ) );
                deriveTypeImpl( selectScope, expanded );
            }
        }

        selectItems.add( expanded );
        aliases.add( alias );

        if ( expanded != null ) {
            inferUnknownTypes( targetType, scope, expanded );
        }
        final AlgDataType type = deriveType( selectScope, expanded );
        setValidatedNodeType( expanded, type );
        fields.add( new AlgDataTypeFieldImpl( 1L, alias, 0, type ) );
        return false;
    }


    private boolean expandStar( List<SqlNode> selectItems, Set<String> aliases, List<AlgDataTypeField> fields, boolean includeSystemVars, SelectScope scope, SqlNode node ) {
        if ( !(node instanceof SqlIdentifier identifier) ) {
            return false;
        }
        if ( !identifier.isStar() ) {
            return false;
        }
        final ParserPos startPosition = identifier.getPos();
        if ( identifier.names.size() == 1 ) {
            boolean hasDynamicStruct = false;
            for ( ScopeChild child : scope.children ) {
                final int before = fields.size();
                if ( child.namespace.getTupleType().isDynamicStruct() ) {
                    hasDynamicStruct = true;
                    // don't expand star if the underneath table is dynamic.
                    // Treat this star as a special field in validation/conversion and wait until execution time to expand this star.
                    final SqlNode exp =
                            new SqlIdentifier(
                                    ImmutableList.of(
                                            child.name,
                                            DynamicRecordType.DYNAMIC_STAR_PREFIX ),
                                    startPosition );
                    addToSelectList(
                            selectItems,
                            aliases,
                            fields,
                            exp,
                            scope,
                            includeSystemVars );
                } else {
                    final SqlNode from = child.namespace.getNode();
                    final SqlValidatorNamespace fromNs = getNamespace( from, scope );
                    assert fromNs != null;
                    final AlgDataType rowType = fromNs.getTupleType();
                    for ( AlgDataTypeField field : rowType.getFields() ) {
                        String columnName = field.getName();

                        // TODO: do real implicit collation here
                        final SqlIdentifier exp =
                                new SqlIdentifier(
                                        ImmutableList.of( child.name, columnName ),
                                        startPosition );
                        // Don't add expanded rolled up columns
                        if ( !isRolledUpColumn( exp, scope ) ) {
                            addOrExpandField(
                                    selectItems,
                                    aliases,
                                    fields,
                                    includeSystemVars,
                                    scope,
                                    exp,
                                    field );
                        }
                    }
                }
                if ( child.nullable ) {
                    for ( int i = before; i < fields.size(); i++ ) {
                        final AlgDataTypeField entry = fields.get( i );
                        final AlgDataType type = entry.getType();
                        if ( !type.isNullable() ) {
                            fields.set( i, new AlgDataTypeFieldImpl( entry.getId(), entry.getName(), i, typeFactory.createTypeWithNullability( type, true ) ) );
                        }
                    }
                }
            }
            // If NATURAL JOIN or USING is present, move key fields to the front of the list, per standard SQL. Disabled if there are dynamic fields.
            if ( !hasDynamicStruct || Bug.CALCITE_2400_FIXED ) {
                new Permute( scope.getNode().getSqlFrom(), 0 ).permute( selectItems, fields );
            }
            return true;
        }

        final SqlIdentifier prefixId = identifier.skipLast( 1 );
        final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();

        scope.resolve( prefixId.names, true, resolved );
        if ( resolved.count() == 0 ) {
            // e.g. "select s.t.* from e" or "select r.* from e"
            throw newValidationError( prefixId, RESOURCE.unknownIdentifier( prefixId.toString() ) );
        }
        final AlgDataType rowType = resolved.only().rowType();
        if ( rowType.isDynamicStruct() ) {
            // don't expand star if the underneath table is dynamic.
            addToSelectList(
                    selectItems,
                    aliases,
                    fields,
                    prefixId.plus( DynamicRecordType.DYNAMIC_STAR_PREFIX, startPosition ),
                    scope,
                    includeSystemVars );
        } else if ( rowType.isStruct() ) {
            for ( AlgDataTypeField field : rowType.getFields() ) {
                String columnName = field.getName();

                // TODO: do real implicit collation here
                addOrExpandField(
                        selectItems,
                        aliases,
                        fields,
                        includeSystemVars,
                        scope,
                        prefixId.plus( columnName, startPosition ),
                        field );
            }
        } else {
            throw newValidationError( prefixId, RESOURCE.starRequiresRecordType() );
        }
        return true;

    }


    private SqlNode maybeCast( SqlNode node, AlgDataType currentType, AlgDataType desiredType ) {
        return currentType.equals( desiredType ) || (currentType.isNullable() != desiredType.isNullable() && typeFactory.createTypeWithNullability( currentType, desiredType.isNullable() ).equals( desiredType ))
                ? node
                : (SqlNode) OperatorRegistry.get( OperatorName.CAST ).createCall( ParserPos.ZERO, node, (Node) SqlTypeUtil.convertTypeToSpec( desiredType ) );
    }


    private boolean addOrExpandField( List<SqlNode> selectItems, Set<String> aliases, List<AlgDataTypeField> fields, boolean includeSystemVars, SelectScope scope, SqlIdentifier id, AlgDataTypeField field ) {
        switch ( field.getType().getStructKind() ) {
            case PEEK_FIELDS:
            case PEEK_FIELDS_DEFAULT:
                final SqlNode starExp = id.plusStar();
                expandStar(
                        selectItems,
                        aliases,
                        fields,
                        includeSystemVars,
                        scope,
                        starExp );
                return true;

            default:
                addToSelectList(
                        selectItems,
                        aliases,
                        fields,
                        id,
                        scope,
                        includeSystemVars );
        }

        return false;
    }


    @Override
    public SqlNode validateSql( SqlNode topNode ) {
        SqlValidatorScope scope = new EmptyScope( this );
        scope = new CatalogScope( scope, ImmutableList.of( "CATALOG" ) );
        final SqlNode topNode2 = validateScopedExpression( topNode, scope );
        final AlgDataType type = getValidatedNodeType( topNode2 );
        Util.discard( type );
        return topNode2;
    }


    @Override
    public List<Moniker> lookupHints( SqlNode topNode, ParserPos pos ) {
        SqlValidatorScope scope = new EmptyScope( this );
        SqlNode outermostNode = performUnconditionalRewrites( topNode, false );
        cursorSet.add( outermostNode );
        if ( outermostNode.isA( Kind.TOP_LEVEL ) ) {
            registerQuery(
                    scope,
                    null,
                    outermostNode,
                    outermostNode,
                    null,
                    false );
        }
        final SqlValidatorNamespace ns = getSqlNamespace( outermostNode );
        if ( ns == null ) {
            throw new AssertionError( "Not a query: " + outermostNode );
        }
        Collection<Moniker> hintList = Sets.newTreeSet( Moniker.COMPARATOR );
        lookupSelectHints( ns, pos, hintList );
        return ImmutableList.copyOf( hintList );
    }


    @Override
    public Moniker lookupQualifiedName( SqlNode topNode, ParserPos pos ) {
        final String posString = pos.toString();
        IdInfo info = idPositions.get( posString );
        if ( info != null ) {
            final SqlQualified qualified = info.scope.fullyQualify( info.id );
            return new SqlIdentifierMoniker( qualified.identifier );
        } else {
            return null;
        }
    }


    /**
     * Looks up completion hints for a syntactically correct select SQL that has been parsed into an expression tree.
     *
     * @param select the Select node of the parsed expression tree
     * @param pos indicates the position in the sql statement we want to get completion hints for
     * @param hintList list of {@link Moniker} (sql identifiers) that can fill in at the indicated position
     */
    void lookupSelectHints( SqlSelect select, ParserPos pos, Collection<Moniker> hintList ) {
        IdInfo info = idPositions.get( pos.toString() );
        if ( (info == null) || (info.scope == null) ) {
            SqlNode fromNode = select.getSqlFrom();
            final SqlValidatorScope fromScope = getFromScope( select );
            lookupFromHints( fromNode, fromScope, pos, hintList );
        } else {
            lookupNameCompletionHints( info.scope, info.id.names, info.id.getPos(), hintList );
        }
    }


    private void lookupSelectHints( SqlValidatorNamespace ns, ParserPos pos, Collection<Moniker> hintList ) {
        final SqlNode node = ns.getNode();
        if ( node instanceof SqlSelect ) {
            lookupSelectHints( (SqlSelect) node, pos, hintList );
        }
    }


    private void lookupFromHints( SqlNode node, SqlValidatorScope scope, ParserPos pos, Collection<Moniker> hintList ) {
        if ( node == null ) {
            // This can happen in cases like "select * _suggest_", so from clause is absent
            return;
        }
        final SqlValidatorNamespace ns = getSqlNamespace( node );
        if ( ns.isWrapperFor( IdentifierNamespace.class ) ) {
            IdentifierNamespace idNs = ns.unwrap( IdentifierNamespace.class );
            final SqlIdentifier id = idNs.getId();
            for ( int i = 0; i < id.names.size(); i++ ) {
                if ( pos.toString().equals( id.getComponent( i ).getPos().toString() ) ) {
                    final List<Moniker> objNames = new ArrayList<>();
                    SqlValidatorUtil.getSchemaObjectMonikers(
                            this.getSnapshot(),
                            id.names.subList( 0, i + 1 ),
                            objNames );
                    for ( Moniker objName : objNames ) {
                        if ( objName.getType() != MonikerType.FUNCTION ) {
                            hintList.add( objName );
                        }
                    }
                    return;
                }
            }
        }
        if ( Objects.requireNonNull( node.getKind() ) == Kind.JOIN ) {
            lookupJoinHints( (SqlJoin) node, scope, pos, hintList );
        } else {
            lookupSelectHints( ns, pos, hintList );
        }
    }


    private void lookupJoinHints( SqlJoin join, SqlValidatorScope scope, ParserPos pos, Collection<Moniker> hintList ) {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        lookupFromHints( left, scope, pos, hintList );
        if ( !hintList.isEmpty() ) {
            return;
        }
        lookupFromHints( right, scope, pos, hintList );
        if ( !hintList.isEmpty() ) {
            return;
        }
        final JoinConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = scopes.get( join );
        if ( Objects.requireNonNull( conditionType ) == JoinConditionType.ON ) {
            condition.findValidOptions( this, joinScope, pos, hintList );
            return;
        }// No suggestions.
        // Not supporting hints for other types such as 'Using' yet.
    }


    /**
     * Populates a list of all the valid alternatives for an identifier.
     *
     * @param scope Validation scope
     * @param names Components of the identifier
     * @param pos position
     * @param hintList a list of valid options
     */
    public final void lookupNameCompletionHints( SqlValidatorScope scope, List<String> names, ParserPos pos, Collection<Moniker> hintList ) {
        // Remove the last part of name - it is a dummy
        List<String> subNames = Util.skipLast( names );

        if ( !subNames.isEmpty() ) {
            // If there's a prefix, resolve it to a namespace.
            SqlValidatorNamespace ns = null;
            for ( String name : subNames ) {
                final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
                scope.resolve( ImmutableList.of( name ), false, resolved );
                if ( resolved.count() == 1 ) {
                    ns = resolved.only().namespace;
                }
                break;
            }
            if ( ns != null ) {
                AlgDataType rowType = ns.getTupleType();
                if ( rowType.isStruct() ) {
                    for ( AlgDataTypeField field : rowType.getFields() ) {
                        hintList.add( new MonikerImpl( field.getName(), MonikerType.COLUMN ) );
                    }
                }
            }

            // builtin function names are valid completion hints when the identifier has only 1 name part
            findAllValidFunctionNames( names, this, hintList, pos );
        } else {
            // No prefix; use the children of the current scope (that is, the aliases in the FROM clause)
            scope.findAliases( hintList );

            // If there's only one alias, add all child columns
            SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope( scope );
            if ( (selectScope != null) && (selectScope.getChildren().size() == 1) ) {
                AlgDataType rowType = selectScope.getChildren().get( 0 ).getTupleType();
                for ( AlgDataTypeField field : rowType.getFields() ) {
                    hintList.add( new MonikerImpl( field.getName(), MonikerType.COLUMN ) );
                }
            }
        }

        findAllValidUdfNames( names, this, hintList );
    }


    private static void findAllValidUdfNames( List<String> names, SqlValidator validator, Collection<Moniker> result ) {
        final List<Moniker> objNames = new ArrayList<>();
        SqlValidatorUtil.getSchemaObjectMonikers( validator.getSnapshot(), names, objNames );
        for ( Moniker objName : objNames ) {
            if ( objName.getType() == MonikerType.FUNCTION ) {
                result.add( objName );
            }
        }
    }


    private static void findAllValidFunctionNames( List<String> names, SqlValidator validator, Collection<Moniker> result, ParserPos pos ) {
        // a function name can only be 1 part
        if ( names.size() > 1 ) {
            return;
        }
        for ( Operator op : validator.getOperatorTable().getOperatorList() ) {
            SqlIdentifier curOpId = new SqlIdentifier( op.getName(), pos );

            final SqlCall call = SqlUtil.makeCall( validator.getOperatorTable(), curOpId );
            if ( call != null ) {
                result.add( new MonikerImpl( op.getName(), MonikerType.FUNCTION ) );
            } else {
                if ( (op.getSyntax() == Syntax.FUNCTION) || (op.getSyntax() == Syntax.PREFIX) ) {
                    if ( ((SqlOperator) op).getOperandTypeChecker() != null ) {
                        String sig = op.getAllowedSignatures();
                        sig = sig.replaceAll( "'", "" );
                        result.add( new MonikerImpl( sig, MonikerType.FUNCTION ) );
                        continue;
                    }
                    result.add( new MonikerImpl( op.getName(), MonikerType.FUNCTION ) );
                }
            }
        }
    }


    @Override
    public SqlNode validateParameterizedExpression( SqlNode topNode, final Map<String, AlgDataType> nameToTypeMap ) {
        SqlValidatorScope scope = new ParameterScope( this, nameToTypeMap );
        return validateScopedExpression( topNode, scope );
    }


    private SqlNode validateScopedExpression( SqlNode topNode, SqlValidatorScope scope ) {
        SqlNode outermostNode = performUnconditionalRewrites( topNode, false );
        cursorSet.add( outermostNode );
        top = outermostNode;
        TRACER.trace( "After unconditional rewrite: {}", outermostNode );
        if ( outermostNode.isA( Kind.TOP_LEVEL ) ) {
            registerQuery( scope, null, outermostNode, outermostNode, null, false );
        }
        outermostNode.validate( this, scope );
        if ( !outermostNode.isA( Kind.TOP_LEVEL ) ) {
            // force type derivation so that we can provide it to the caller later without needing the scope
            deriveType( scope, outermostNode );
        }
        TRACER.trace( "After validation: {}", outermostNode );
        return outermostNode;
    }


    @Override
    public void validateQuery( SqlNode node, SqlValidatorScope scope, AlgDataType targetRowType ) {
        final SqlValidatorNamespace ns = getNamespace( node, scope );
        if ( node.getKind() == Kind.TABLESAMPLE ) {
            List<Node> operands = ((SqlCall) node).getOperandList();
            SqlSampleSpec sampleSpec = SqlLiteral.sampleValue( (SqlNode) operands.get( 1 ) );
            if ( sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec ) {
                validateFeature( RESOURCE.sQLFeature_T613(), node.getPos() );
            } else if ( sampleSpec instanceof SqlSampleSpec.SqlSubstitutionSampleSpec ) {
                validateFeature( RESOURCE.sQLFeatureExt_T613_Substitution(), node.getPos() );
            }
        }

        validateNamespace( ns, targetRowType );
        if ( Objects.requireNonNull( node.getKind() ) == Kind.EXTEND ) {// Until we have a dedicated namespace for EXTEND
            deriveType( scope, node );
        }
        if ( node == top ) {
            validateModality( node );
        }
        validateAccess( node, ns.getEntity(), AccessEnum.SELECT );
    }


    /**
     * Validates a namespace.
     *
     * @param namespace Namespace
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     */
    protected void validateNamespace( final SqlValidatorNamespace namespace, AlgDataType targetRowType ) {
        namespace.validate( targetRowType );
        if ( namespace.getNode() != null ) {
            setValidatedNodeType( namespace.getNode(), namespace.getType() );
        }
    }


    @VisibleForTesting
    public SqlValidatorScope getEmptyScope() {
        return new EmptyScope( this );
    }


    public SqlValidatorScope getCursorScope( SqlSelect select ) {
        return cursorScopes.get( select );
    }


    @Override
    public SqlValidatorScope getWhereScope( SqlSelect select ) {
        return whereScopes.get( select );
    }


    @Override
    public SqlValidatorScope getSelectScope( SqlSelect select ) {
        return selectScopes.get( select );
    }


    @Override
    public SelectScope getRawSelectScope( SqlSelect select ) {
        SqlValidatorScope scope = getSelectScope( select );
        if ( scope instanceof AggregatingSelectScope ) {
            scope = ((AggregatingSelectScope) scope).getParent();
        }
        return (SelectScope) scope;
    }


    @Override
    public SqlValidatorScope getHavingScope( SqlSelect select ) {
        // Yes, it's the same as getSelectScope
        return selectScopes.get( select );
    }


    @Override
    public SqlValidatorScope getGroupScope( SqlSelect select ) {
        // Yes, it's the same as getWhereScope
        return groupByScopes.get( select );
    }


    @Override
    public SqlValidatorScope getFromScope( SqlSelect select ) {
        return scopes.get( select );
    }


    @Override
    public SqlValidatorScope getOrderScope( SqlSelect select ) {
        return orderScopes.get( select );
    }


    @Override
    public SqlValidatorScope getMatchRecognizeScope( SqlMatchRecognize node ) {
        return scopes.get( node );
    }


    @Override
    public SqlValidatorScope getJoinScope( SqlNode node ) {
        return scopes.get( SqlUtil.stripAs( node ) );
    }


    @Override
    public SqlValidatorScope getOverScope( SqlNode node ) {
        return scopes.get( node );
    }


    private SqlValidatorNamespace getNamespace( SqlNode node, SqlValidatorScope scope ) {
        if ( node instanceof SqlIdentifier id && scope instanceof DelegatingScope ) {
            final DelegatingScope idScope = (DelegatingScope) ((DelegatingScope) scope).getParent();
            return getNamespace( id, idScope );
        } else if ( node instanceof SqlCall call ) {
            // Handle extended identifiers.
            switch ( call.getOperator().getKind() ) {
                case EXTEND:
                    final SqlIdentifier id = (SqlIdentifier) call.getOperandList().get( 0 );
                    final DelegatingScope idScope = (DelegatingScope) scope;
                    return getNamespace( id, idScope );
                case AS:
                    final Node nested = call.getOperandList().get( 0 );
                    if ( Objects.requireNonNull( nested.getKind() ) == Kind.EXTEND ) {
                        return getNamespace( (SqlIdentifier) nested, scope );
                    }
                    break;
            }
        }
        return getSqlNamespace( node );
    }


    private SqlValidatorNamespace getNamespace( SqlIdentifier id, DelegatingScope scope ) {
        if ( id.isSimple() ) {
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            scope.resolve( id.names, false, resolved );
        }
        return getSqlNamespace( id );
    }


    @Override
    public ValidatorNamespace getNamespace( Node node ) {
        return getSqlNamespace( (SqlNode) node );
    }


    @Override
    public SqlValidatorNamespace getSqlNamespace( SqlNode node ) {
        switch ( node.getKind() ) {
            case AS:
                // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
                final SqlValidatorNamespace ns = namespaces.get( node );
                if ( ns != null ) {
                    return ns;
                }
                // fall through
            case OVER:
            case COLLECTION_TABLE:
            case ORDER_BY:
            case TABLESAMPLE:
                return getSqlNamespace( ((SqlCall) node).operand( 0 ) );
            default:
                return namespaces.get( node );
        }
    }


    private void handleOffsetFetch( SqlNode offset, SqlNode fetch ) {
        if ( offset instanceof SqlDynamicParam ) {
            setValidatedNodeType( offset, typeFactory.createPolyType( PolyType.INTEGER ) );
        }
        if ( fetch instanceof SqlDynamicParam ) {
            setValidatedNodeType( fetch, typeFactory.createPolyType( PolyType.INTEGER ) );
        }
    }


    /**
     * Performs expression rewrites which are always used unconditionally. These rewrites massage the expression tree into a standard form so that the rest of the validation logic can be simpler.
     *
     * @param node expression to be rewritten
     * @param underFrom whether node appears directly under a FROM clause
     * @return rewritten expression
     */
    protected SqlNode performUnconditionalRewrites( SqlNode node, boolean underFrom ) {
        if ( node == null ) {
            return null;
        }

        SqlNode newOperand;

        // first transform operands and invoke generic call rewrite
        if ( node instanceof SqlCall call ) {
            if ( node instanceof SqlMerge ) {
                validatingSqlMerge = true;
            }
            final Kind kind = call.getKind();
            final List<Node> operands = call.getOperandList();
            for ( int i = 0; i < operands.size(); i++ ) {
                Node operand = operands.get( i );
                boolean childUnderFrom;
                if ( kind == Kind.SELECT ) {
                    childUnderFrom = i == SqlSelect.FROM_OPERAND;
                } else if ( kind == Kind.AS && (i == 0) ) {
                    // for an aliased expression, it is under FROM if the AS expression is under FROM
                    childUnderFrom = underFrom;
                } else {
                    childUnderFrom = false;
                }
                newOperand = performUnconditionalRewrites( (SqlNode) operand, childUnderFrom );
                if ( newOperand != null && newOperand != operand ) {
                    call.setOperand( i, newOperand );
                }
            }

            if ( call.getOperator() instanceof SqlUnresolvedFunction function ) {
                assert call instanceof SqlBasicCall;
                // This function hasn't been resolved yet.  Perform a half-hearted resolution now in case it's a builtin function requiring special casing.
                // If it's not, we'll handle it later during overload resolution.
                final List<Operator> overloads = new ArrayList<>();
                opTab.lookupOperatorOverloads( function.getNameAsId(), function.getFunctionCategory(), SqlSyntax.FUNCTION.getSyntax(), overloads );
                if ( overloads.size() == 1 ) {
                    ((SqlBasicCall) call).setOperator( (SqlOperator) overloads.get( 0 ) );
                }
            }
            if ( rewriteCalls ) {
                node = ((SqlOperator) call.getOperator()).rewriteCall( this, call );
            }
        } else if ( node instanceof SqlNodeList list ) {
            for ( int i = 0, count = list.size(); i < count; i++ ) {
                SqlNode operand = (SqlNode) list.get( i );
                newOperand = performUnconditionalRewrites( operand, false );
                if ( newOperand != null ) {
                    list.set( i, newOperand );
                }
            }
        }

        // now transform node itself
        final Kind kind = node.getKind();
        switch ( kind ) {
            case VALUES:
                if ( underFrom || true ) {
                    // leave FROM (VALUES(...)) [ AS alias ] clauses alone, otherwise they grow cancerously if this rewrite is invoked over and over
                    return node;
                } else {
                    final SqlNodeList selectList = new SqlNodeList( ParserPos.ZERO );
                    selectList.add( SqlIdentifier.star( ParserPos.ZERO ) );
                    return new SqlSelect(
                            node.getPos(),
                            null,
                            selectList,
                            node,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null );
                }

            case ORDER_BY: {
                SqlOrderBy orderBy = (SqlOrderBy) node;
                handleOffsetFetch( orderBy.offset, orderBy.fetch );
                if ( orderBy.query instanceof SqlSelect select ) {

                    // Don't clobber existing ORDER BY.  It may be needed for an order-sensitive function like RANK.
                    if ( select.getOrderList() == null ) {
                        // push ORDER BY into existing select
                        select.setOrderBy( orderBy.orderList );
                        select.setOffset( orderBy.offset );
                        select.setFetch( orderBy.fetch );
                        return select;
                    }
                }
                if ( orderBy.query instanceof SqlWith with && ((SqlWith) orderBy.query).body instanceof SqlSelect ) {
                    SqlSelect select = (SqlSelect) with.body;

                    // Don't clobber existing ORDER BY.  It may be needed for an order-sensitive function like RANK.
                    if ( select.getOrderList() == null ) {
                        // push ORDER BY into existing select
                        select.setOrderBy( orderBy.orderList );
                        select.setOffset( orderBy.offset );
                        select.setFetch( orderBy.fetch );
                        return with;
                    }
                }
                final SqlNodeList selectList = new SqlNodeList( ParserPos.ZERO );
                selectList.add( SqlIdentifier.star( ParserPos.ZERO ) );
                final SqlNodeList orderList;
                if ( getInnerSelect( node ) != null && isAggregate( getInnerSelect( node ) ) ) {
                    orderList = Node.clone( orderBy.orderList );
                    // We assume that ORDER BY item does not have ASC etc.
                    // We assume that ORDER BY item is present in SELECT list.
                    for ( int i = 0; i < orderList.size(); i++ ) {
                        SqlNode sqlNode = (SqlNode) orderList.get( i );
                        SqlNodeList selectList2 = getInnerSelect( node ).getSqlSelectList();
                        for ( Ord<Node> sel : Ord.zip( selectList2 ) ) {
                            if ( SqlUtil.stripAs( (SqlNode) sel.e ).equalsDeep( sqlNode, Litmus.IGNORE ) ) {
                                orderList.set( i, SqlLiteral.createExactNumeric( Integer.toString( sel.i + 1 ), ParserPos.ZERO ) );
                            }
                        }
                    }
                } else {
                    orderList = orderBy.orderList;
                }
                return new SqlSelect(
                        ParserPos.ZERO,
                        null,
                        selectList,
                        orderBy.query,
                        null,
                        null,
                        null,
                        null,
                        orderList,
                        orderBy.offset,
                        orderBy.fetch );
            }

            case EXPLICIT_TABLE: {
                // (TABLE t) is equivalent to (SELECT * FROM t)
                SqlCall call = (SqlCall) node;
                final SqlNodeList selectList = new SqlNodeList( ParserPos.ZERO );
                selectList.add( SqlIdentifier.star( ParserPos.ZERO ) );
                return new SqlSelect(
                        ParserPos.ZERO,
                        null,
                        selectList,
                        call.operand( 0 ),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null );
            }

            case DELETE: {
                SqlDelete call = (SqlDelete) node;
                SqlSelect select = createSourceSelectForDelete( call );
                call.setSourceSelect( select );
                break;
            }

            case UPDATE: {
                SqlUpdate call = (SqlUpdate) node;
                SqlSelect select = createSourceSelectForUpdate( call );
                call.setSourceSelect( select );

                // See if we're supposed to rewrite UPDATE to MERGE (unless this is the UPDATE clause of a MERGE, in which case leave it alone).
                if ( !validatingSqlMerge ) {
                    SqlNode selfJoinSrcExpr = getSelfJoinExprForUpdate( call.getTargetTable(), UPDATE_SRC_ALIAS );
                    if ( selfJoinSrcExpr != null ) {
                        node = rewriteUpdateToMerge( call, selfJoinSrcExpr );
                    }
                }
                break;
            }

            case MERGE: {
                SqlMerge call = (SqlMerge) node;
                rewriteMerge( call );
                break;
            }
        }
        return node;
    }


    private SqlSelect getInnerSelect( SqlNode node ) {
        for ( ; ; ) {
            if ( node instanceof SqlSelect ) {
                return (SqlSelect) node;
            } else if ( node instanceof SqlOrderBy ) {
                node = ((SqlOrderBy) node).query;
            } else if ( node instanceof SqlWith ) {
                node = ((SqlWith) node).body;
            } else {
                return null;
            }
        }
    }


    private void rewriteMerge( SqlMerge call ) {
        SqlNodeList selectList;
        SqlUpdate updateStmt = call.getUpdateCall();
        if ( updateStmt != null ) {
            // if we have an update statement, just clone the select list from the update statement's source since it's the same as
            // what we want for the select list of the merge source -- '*' followed by the update set expressions
            selectList = Node.clone( updateStmt.getSourceSelect().getSqlSelectList() );
        } else {
            // otherwise, just use select *
            selectList = new SqlNodeList( ParserPos.ZERO );
            selectList.add( SqlIdentifier.star( ParserPos.ZERO ) );
        }
        SqlNode targetTable = call.getTargetTable();
        if ( call.getAlias() != null ) {
            targetTable = (SqlNode) SqlValidatorUtil.addAlias( targetTable, call.getAlias().getSimple() );
        }

        // Provided there is an insert substatement, the source select for the merge is a left outer join between the source in the USING clause and the target table; otherwise, the join is just an
        // inner join.  Need to clone the source table reference in order for validation to work
        SqlNode sourceTableRef = call.getSourceTableRef();
        SqlInsert insertCall = call.getInsertCall();
        JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;
        final SqlNode leftJoinTerm = Node.clone( sourceTableRef );
        SqlNode outerJoin =
                new SqlJoin(
                        ParserPos.ZERO,
                        leftJoinTerm,
                        SqlLiteral.createBoolean( false, ParserPos.ZERO ),
                        SqlLiteral.createSymbol( joinType, ParserPos.ZERO ),
                        targetTable,
                        SqlLiteral.createSymbol( JoinConditionType.ON, ParserPos.ZERO ),
                        call.getCondition() );
        SqlSelect select =
                new SqlSelect(
                        ParserPos.ZERO,
                        null,
                        selectList,
                        outerJoin,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null );
        call.setSourceSelect( select );

        // Source for the insert call is a select of the source table reference with the select list being the value expressions;
        // note that the values clause has already been converted to a select on the values row constructor; so we need to extract that via the from clause on the select
        if ( insertCall != null ) {
            SqlCall valuesCall = (SqlCall) insertCall.getSource();
            SqlCall rowCall = valuesCall.operand( 0 );
            selectList =
                    new SqlNodeList(
                            rowCall.getSqlOperandList(),
                            ParserPos.ZERO );
            final SqlNode insertSource = Node.clone( sourceTableRef );
            select =
                    new SqlSelect(
                            ParserPos.ZERO,
                            null,
                            selectList,
                            insertSource,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null );
            insertCall.setSource( select );
        }
    }


    private SqlNode rewriteUpdateToMerge( SqlUpdate updateCall, SqlNode selfJoinSrcExpr ) {
        // Make sure target has an alias.
        if ( updateCall.getAlias() == null ) {
            updateCall.setAlias( new SqlIdentifier( UPDATE_TGT_ALIAS, ParserPos.ZERO ) );
        }
        SqlNode selfJoinTgtExpr =
                getSelfJoinExprForUpdate(
                        updateCall.getTargetTable(),
                        updateCall.getAlias().getSimple() );
        assert selfJoinTgtExpr != null;

        // Create join condition between source and target exprs, creating a conjunction with the user-level WHERE clause if one was supplied
        SqlNode condition = updateCall.getCondition();
        SqlNode selfJoinCond =
                (SqlNode) OperatorRegistry.get( OperatorName.EQUALS ).createCall(
                        ParserPos.ZERO,
                        selfJoinSrcExpr,
                        selfJoinTgtExpr );
        if ( condition == null ) {
            condition = selfJoinCond;
        } else {
            condition =
                    (SqlNode) OperatorRegistry.get( OperatorName.AND ).createCall(
                            ParserPos.ZERO,
                            selfJoinCond,
                            condition );
        }
        SqlNode target = (SqlNode) updateCall.getTargetTable().clone( ParserPos.ZERO );

        // For the source, we need to anonymize the fields, so that for a statement like UPDATE T SET I = I + 1, there's no ambiguity for the "I" in "I + 1";
        // this is OK because the source and target have identical values due to the self-join.
        // Note that we anonymize the source rather than the target because downstream, the optimizer rules don't want to see any projection on top of the target.
        IdentifierNamespace ns = new IdentifierNamespace( this, target, null, null );
        AlgDataType rowType = ns.getTupleType();
        SqlNode source = (SqlNode) updateCall.getTargetTable().clone( ParserPos.ZERO );
        final SqlNodeList selectList = new SqlNodeList( ParserPos.ZERO );
        int i = 1;
        for ( AlgDataTypeField field : rowType.getFields() ) {
            SqlIdentifier col = new SqlIdentifier( field.getName(), ParserPos.ZERO );
            selectList.add( SqlValidatorUtil.addAlias( col, UPDATE_ANON_PREFIX + i ) );
            ++i;
        }
        source = new SqlSelect(
                ParserPos.ZERO,
                null,
                selectList,
                source,
                null,
                null,
                null,
                null,
                null,
                null,
                null );
        source = (SqlNode) SqlValidatorUtil.addAlias( source, UPDATE_SRC_ALIAS );
        SqlMerge mergeCall =
                new SqlMerge(
                        updateCall.getPos(),
                        target,
                        condition,
                        source,
                        updateCall,
                        null,
                        null,
                        updateCall.getAlias() );
        rewriteMerge( mergeCall );
        return mergeCall;
    }


    /**
     * Allows a subclass to provide information about how to convert an UPDATE into a MERGE via self-join. If this method returns null, then no such conversion takes place. Otherwise, this method should return a suitable
     * unique identifier expression for the given table.
     *
     * @param table identifier for table being updated
     * @param alias alias to use for qualifying columns in expression, or null for unqualified references; if this is equal to {@value #UPDATE_SRC_ALIAS}, then column references have been anonymized to "SYS$ANONx", where x is the 1-based column number.
     * @return expression for unique identifier, or null to prevent conversion
     */
    protected SqlNode getSelfJoinExprForUpdate( SqlNode table, String alias ) {
        return null;
    }


    /**
     * Creates the SELECT statement that putatively feeds rows into an UPDATE statement to be updated.
     *
     * @param call Call to the UPDATE operator
     * @return select statement
     */
    protected SqlSelect createSourceSelectForUpdate( SqlUpdate call ) {
        final SqlNodeList selectList = new SqlNodeList( ParserPos.ZERO );
        selectList.add( SqlIdentifier.star( ParserPos.ZERO ) );
        int ordinal = 0;
        for ( SqlNode exp : call.getSourceExpressionList().getSqlList() ) {
            // Force unique aliases to avoid a duplicate for Y with SET X=Y
            String alias = CoreUtil.deriveAliasFromOrdinal( ordinal );
            selectList.add( SqlValidatorUtil.addAlias( exp, alias ) );
            ++ordinal;
        }
        SqlNode sourceTable = call.getTargetTable();
        if ( call.getAlias() != null ) {
            sourceTable =
                    (SqlNode) SqlValidatorUtil.addAlias(
                            sourceTable,
                            call.getAlias().getSimple() );
        }
        return new SqlSelect(
                ParserPos.ZERO,
                null,
                selectList,
                sourceTable,
                call.getCondition(),
                null,
                null,
                null,
                null,
                null,
                null );
    }


    /**
     * Creates the SELECT statement that putatively feeds rows into a DELETE statement to be deleted.
     *
     * @param call Call to the DELETE operator
     * @return select statement
     */
    protected SqlSelect createSourceSelectForDelete( SqlDelete call ) {
        final SqlNodeList selectList = new SqlNodeList( ParserPos.ZERO );
        selectList.add( SqlIdentifier.star( ParserPos.ZERO ) );
        SqlNode sourceTable = call.getTargetTable();
        if ( call.getAlias() != null ) {
            sourceTable =
                    (SqlNode) SqlValidatorUtil.addAlias(
                            sourceTable,
                            call.getAlias().getSimple() );
        }
        return new SqlSelect(
                ParserPos.ZERO,
                null,
                selectList,
                sourceTable,
                call.getCondition(),
                null,
                null,
                null,
                null,
                null,
                null );
    }


    /**
     * Returns null if there is no common type. E.g. if the rows have a different number of columns.
     */
    AlgDataType getTableConstructorRowType( SqlCall values, SqlValidatorScope scope ) {
        final List<Node> rows = values.getOperandList();
        assert !rows.isEmpty();
        final List<AlgDataType> rowTypes = new ArrayList<>();
        for ( final Node row : rows ) {
            assert row.getKind() == Kind.ROW;
            SqlCall rowConstructor = (SqlCall) row;

            // REVIEW jvs: Once we support single-row queries as rows, need to infer aliases from there.
            final List<String> aliasList = new ArrayList<>();
            final List<AlgDataType> types = new ArrayList<>();
            final List<Long> ids = new ArrayList<>();
            for ( Ord<Node> column : Ord.zip( rowConstructor.getOperandList() ) ) {
                final String alias = deriveAlias( (SqlNode) column.e, column.i );
                aliasList.add( alias );
                final AlgDataType type = deriveType( scope, column.e );
                types.add( type );
                ids.add( null );
            }
            rowTypes.add( typeFactory.createStructType( ids, types, aliasList ) );
        }
        if ( rows.size() == 1 ) {
            // TODO jvs: Get rid of this workaround once leastRestrictive can handle all cases
            return rowTypes.get( 0 );
        }
        return typeFactory.leastRestrictive( rowTypes );
    }


    @Override
    public AlgDataType getValidatedNodeType( Node node ) {
        AlgDataType type = getValidatedNodeTypeIfKnown( (SqlNode) node );
        if ( type == null ) {
            throw Util.needToImplement( node );
        } else {
            return type;
        }
    }


    @Override
    public AlgDataType getValidatedNodeTypeIfKnown( SqlNode node ) {
        final AlgDataType type = nodeToTypeMap.get( node );
        if ( type != null ) {
            return type;
        }
        final SqlValidatorNamespace ns = getSqlNamespace( node );
        if ( ns != null ) {
            return ns.getType();
        }
        final SqlNode original = originalExprs.get( node );
        if ( original != null && original != node ) {
            return getValidatedNodeType( original );
        }
        if ( node instanceof SqlIdentifier ) {
            return SqlUtil.getNamedType( (SqlIdentifier) node, snapshot );
        }
        return null;
    }


    /**
     * Saves the type of a {@link SqlNode}, now that it has been validated.
     *
     * @param node A SQL parse tree node, never null
     * @param type Its type; must not be null
     */
    public void setValidatedNodeType( SqlNode node, AlgDataType type ) {
        Objects.requireNonNull( type );
        Objects.requireNonNull( node );
        if ( type.equals( unknownType ) ) {
            // don't set anything until we know what it is, and don't overwrite a known type with the unknown type
            return;
        }
        nodeToTypeMap.put( node, type );
    }


    @Override
    public void removeValidatedNodeType( SqlNode node ) {
        nodeToTypeMap.remove( node );
    }


    @Override
    public AlgDataType deriveType( ValidatorScope scope, Node expr ) {
        Objects.requireNonNull( scope );
        Objects.requireNonNull( expr );

        // if we already know the type, no need to re-derive
        AlgDataType type = nodeToTypeMap.get( expr );
        if ( type != null ) {
            return type;
        }
        final SqlValidatorNamespace ns = getSqlNamespace( (SqlNode) expr );
        if ( ns != null ) {
            return ns.getType();
        }
        type = deriveTypeImpl( (SqlValidatorScope) scope, (SqlNode) expr );
        Preconditions.checkArgument( type != null, "SqlValidator.deriveTypeInternal returned null" );
        setValidatedNodeType( (SqlNode) expr, type );
        return type;
    }


    /**
     * Derives the type of a node, never null.
     */
    AlgDataType deriveTypeImpl( SqlValidatorScope scope, SqlNode operand ) {
        DeriveTypeVisitor v = new DeriveTypeVisitor( scope );
        final AlgDataType type = operand.accept( v );
        return Objects.requireNonNull( scope.nullifyType( operand, type ) );
    }


    @Override
    public AlgDataType deriveConstructorType( SqlValidatorScope scope, SqlCall call, SqlFunction unresolvedConstructor, SqlFunction resolvedConstructor, List<AlgDataType> argTypes ) {
        SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
        assert sqlIdentifier != null;
        AlgDataType type = SqlUtil.getNamedType( sqlIdentifier, snapshot );
        if ( type == null ) {
            // TODO: Proper type name formatting
            throw newValidationError( sqlIdentifier, RESOURCE.unknownDatatypeName( sqlIdentifier.toString() ) );
        }

        if ( resolvedConstructor == null ) {
            if ( call.operandCount() > 0 ) {
                // This is not a default constructor invocation, and no user-defined constructor could be found
                throw handleUnresolvedFunction( call, unresolvedConstructor, argTypes, null );
            }
        } else {
            SqlCall testCall = (SqlCall) resolvedConstructor.createCall( call.getPos(), call.getOperandList() );
            AlgDataType returnType = resolvedConstructor.validateOperands( this, scope, testCall );
            assert type == returnType;
        }

        if ( shouldExpandIdentifiers() ) {
            // fake a fully-qualified call to the default constructor
            ((SqlBasicCall) call).setOperator( Objects.requireNonNullElseGet( resolvedConstructor, () -> new SqlFunction(
                    new SqlIdentifier( type.getFieldNames(), ParserPos.ZERO ),
                    ReturnTypes.explicit( type ),
                    null,
                    null,
                    null,
                    FunctionCategory.USER_DEFINED_CONSTRUCTOR ) ) );
        }
        return type;
    }


    @Override
    public PolyphenyDbException handleUnresolvedFunction( SqlCall call, SqlFunction unresolvedFunction, List<AlgDataType> argTypes, List<String> argNames ) {
        // For builtins, we can give a better error message
        final List<Operator> overloads = new ArrayList<>();
        opTab.lookupOperatorOverloads( unresolvedFunction.getNameAsId(), null, SqlSyntax.FUNCTION.getSyntax(), overloads );
        if ( overloads.size() == 1 ) {
            SqlFunction fun = (SqlFunction) overloads.get( 0 );
            if ( (fun.getSqlIdentifier() == null) && (fun.getSqlSyntax() != SqlSyntax.FUNCTION_ID) ) {
                final int expectedArgCount = fun.getOperandCountRange().getMin();
                throw newValidationError( call, RESOURCE.invalidArgCount( call.getOperator().getName(), expectedArgCount ) );
            }
        }

        AssignableOperandTypeChecker typeChecking = new AssignableOperandTypeChecker( argTypes, argNames );
        String signature = typeChecking.getAllowedSignatures( unresolvedFunction, unresolvedFunction.getName() );
        throw newValidationError( call, RESOURCE.validatorUnknownFunction( signature ) );
    }


    protected void inferUnknownTypes( @Nonnull AlgDataType inferredType, @Nonnull SqlValidatorScope scope, @Nonnull SqlNode node ) {
        Objects.requireNonNull( inferredType );
        Objects.requireNonNull( scope );
        Objects.requireNonNull( node );
        final SqlValidatorScope newScope = scopes.get( node );
        if ( newScope != null ) {
            scope = newScope;
        }
        boolean isNullLiteral = CoreUtil.isNullLiteral( node, false );
        if ( (node instanceof SqlDynamicParam) || isNullLiteral ) {
            if ( inferredType.equals( unknownType ) ) {
                if ( isNullLiteral ) {
                    throw newValidationError( node, RESOURCE.nullIllegal() );
                } else {
                    throw newValidationError( node, RESOURCE.dynamicParamIllegal() );
                }
            }

            // REVIEW:  should dynamic parameter types always be nullable? no?
            AlgDataType newInferredType = inferredType;//typeFactory.createTypeWithNullability( inferredType, true );
            if ( PolyTypeUtil.inCharFamily( inferredType ) ) {
                newInferredType = typeFactory.createTypeWithCharsetAndCollation( newInferredType, inferredType.getCharset(), inferredType.getCollation() );
            }
            setValidatedNodeType( node, newInferredType );
        } else if ( node instanceof SqlNodeList nodeList ) {
            if ( inferredType.isStruct() ) {
                if ( inferredType.getFieldCount() != nodeList.size() ) {
                    // this can happen when we're validating an INSERT
                    // where the source and target degrees are different;
                    // bust out, and the error will be detected higher up
                    return;
                }
            }
            int i = 0;
            for ( Node child : nodeList ) {
                AlgDataType type;
                if ( inferredType.isStruct() ) {
                    type = inferredType.getFields().get( i ).getType();
                    ++i;
                } else {
                    type = inferredType;
                }
                inferUnknownTypes( type, scope, (SqlNode) child );
            }
        } else if ( node instanceof SqlCase caseCall ) {

            final AlgDataType whenType = caseCall.getValueOperand() == null ? booleanType : unknownType;
            for ( SqlNode sqlNode : caseCall.getWhenOperands().getSqlList() ) {
                inferUnknownTypes( whenType, scope, sqlNode );
            }
            AlgDataType returnType = deriveType( scope, node );
            for ( SqlNode sqlNode : caseCall.getThenOperands().getSqlList() ) {
                inferUnknownTypes( returnType, scope, sqlNode );
            }

            if ( !CoreUtil.isNullLiteral( caseCall.getElseOperand(), false ) ) {
                inferUnknownTypes( returnType, scope, caseCall.getElseOperand() );
            } else {
                setValidatedNodeType( caseCall.getElseOperand(), returnType );
            }
        } else if ( node.getKind() == Kind.AS ) {
            // For AS operator, only infer the operand not the alias
            inferUnknownTypes( inferredType, scope, ((SqlCall) node).operand( 0 ) );
        } else if ( node instanceof SqlCall call ) {
            final PolyOperandTypeInference operandTypeInference = ((SqlOperator) call.getOperator()).getOperandTypeInference();
            final SqlCallBinding callBinding = new SqlCallBinding( this, scope, call );
            final List<? extends Node> operands = callBinding.operands();
            final AlgDataType[] operandTypes = new AlgDataType[operands.size()];
            Arrays.fill( operandTypes, unknownType );
            // TODO:  eventually should assert(operandTypeInference != null) instead; for now just eat it
            if ( operandTypeInference != null ) {
                operandTypeInference.inferOperandTypes( callBinding, inferredType, operandTypes );
            }
            for ( int i = 0; i < operands.size(); ++i ) {
                final Node operand = operands.get( i );
                if ( operand != null ) {
                    inferUnknownTypes( operandTypes[i], scope, (SqlNode) operand );
                }
            }
        }
    }


    /**
     * Adds an expression to a select list, ensuring that its alias does not clash with any existing expressions on the list.
     */
    protected void addToSelectList( List<SqlNode> list, Set<String> aliases, List<AlgDataTypeField> fieldList, SqlNode exp, SqlValidatorScope scope, final boolean includeSystemVars ) {
        String alias = SqlValidatorUtil.getAlias( exp, -1 );
        String uniqueAlias = ValidatorUtil.uniquify( alias, aliases, ValidatorUtil.EXPR_SUGGESTER );
        if ( !alias.equals( uniqueAlias ) ) {
            exp = (SqlNode) SqlValidatorUtil.addAlias( exp, uniqueAlias );
        }
        fieldList.add( new AlgDataTypeFieldImpl( 1L, uniqueAlias, fieldList.size(), deriveType( scope, exp ) ) );
        list.add( exp );
    }


    @Override
    public String deriveAlias( SqlNode node, int ordinal ) {
        return SqlValidatorUtil.getAlias( node, ordinal );
    }


    // implement SqlValidator
    @Override
    public void setIdentifierExpansion( boolean expandIdentifiers ) {
        this.expandIdentifiers = expandIdentifiers;
    }


    // implement SqlValidator
    @Override
    public void setColumnReferenceExpansion( boolean expandColumnReferences ) {
        this.expandColumnReferences = expandColumnReferences;
    }


    // implement SqlValidator
    @Override
    public boolean getColumnReferenceExpansion() {
        return expandColumnReferences;
    }


    @Override
    public void setDefaultNullCollation( NullCollation nullCollation ) {
        this.nullCollation = Objects.requireNonNull( nullCollation );
    }


    @Override
    public NullCollation getDefaultNullCollation() {
        return nullCollation;
    }


    // implement SqlValidator
    @Override
    public void setCallRewrite( boolean rewriteCalls ) {
        this.rewriteCalls = rewriteCalls;
    }


    @Override
    public boolean shouldExpandIdentifiers() {
        return expandIdentifiers;
    }


    protected boolean shouldAllowIntermediateOrderBy() {
        return true;
    }


    private void registerMatchRecognize( SqlValidatorScope parentScope, SqlValidatorScope usingScope, SqlMatchRecognize call, SqlNode enclosingNode, String alias, boolean forceNullable ) {

        final MatchRecognizeNamespace matchRecognizeNamespace = createMatchRecognizeNameSpace( call, enclosingNode );
        registerNamespace( usingScope, alias, matchRecognizeNamespace, forceNullable );

        final MatchRecognizeScope matchRecognizeScope = new MatchRecognizeScope( parentScope, call );
        scopes.put( call, matchRecognizeScope );

        // parse input query
        SqlNode expr = call.getTableRef();
        SqlNode newExpr =
                registerFrom(
                        usingScope,
                        matchRecognizeScope,
                        true,
                        expr,
                        expr,
                        null,
                        null,
                        forceNullable,
                        false );
        if ( expr != newExpr ) {
            call.setOperand( 0, newExpr );
        }
    }


    protected MatchRecognizeNamespace createMatchRecognizeNameSpace( SqlMatchRecognize call, SqlNode enclosingNode ) {
        return new MatchRecognizeNamespace( this, call, enclosingNode );
    }


    /**
     * Registers a new namespace, and adds it as a child of its parent scope.
     * Derived class can override this method to tinker with namespaces as they are created.
     *
     * @param usingScope Parent scope (which will want to look for things in this namespace)
     * @param alias Alias by which parent will refer to this namespace
     * @param ns Namespace
     * @param forceNullable Whether to force the type of namespace to be nullable
     */
    protected void registerNamespace( SqlValidatorScope usingScope, String alias, SqlValidatorNamespace ns, boolean forceNullable ) {
        namespaces.put( ns.getNode(), ns );
        if ( usingScope != null ) {
            usingScope.addChild( ns, alias, forceNullable );
        }
    }


    /**
     * Registers scopes and namespaces implied a relational expression in the FROM clause.
     * <p>
     * {@code parentScope} and {@code usingScope} are often the same. They differ when the namespace are not visible within the parent. (Example needed.)
     * <p>
     * Likewise, {@code enclosingNode} and {@code node} are often the same. {@code enclosingNode} is the topmost node within the FROM clause, from which any decorations like an alias (<code>AS alias</code>) or a table
     * sample clause are stripped away to get {@code node}. Both are recorded in the namespace.
     *
     * @param parentScope Parent scope which this scope turns to in order to resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param register Whether to register this scope as a child of {@code usingScope}
     * @param node Node which namespace is based on
     * @param enclosingNode Outermost node for namespace, including decorations such as alias and sample clause
     * @param alias Alias
     * @param extendList Definitions of extended columns
     * @param forceNullable Whether to force the type of namespace to be nullable because it is in an outer join
     * @param lateral Whether LATERAL is specified, so that items to the left of this in the JOIN tree are visible in the scope
     * @return registered node, usually the same as {@code node}
     */
    private SqlNode registerFrom( SqlValidatorScope parentScope, SqlValidatorScope usingScope, boolean register, final SqlNode node, SqlNode enclosingNode, String alias, SqlNodeList extendList, boolean forceNullable, final boolean lateral ) {
        final Kind kind = node.getKind();

        SqlNode expr;
        SqlNode newExpr;

        // Add an alias if necessary.
        SqlNode newNode = node;
        if ( alias == null ) {
            switch ( kind ) {
                case IDENTIFIER:
                case OVER:
                    alias = deriveAlias( node, -1 );
                    if ( alias == null ) {
                        alias = deriveAlias( node, nextGeneratedId++ );
                    }
                    if ( shouldExpandIdentifiers() ) {
                        newNode = (SqlNode) SqlValidatorUtil.addAlias( node, alias );
                    }
                    break;

                case SELECT:
                case UNION:
                case INTERSECT:
                case EXCEPT:
                case VALUES:
                case UNNEST:
                case OTHER_FUNCTION:
                case COLLECTION_TABLE:
                case MATCH_RECOGNIZE:

                    // give this anonymous construct a name since later query processing stages rely on it
                    alias = deriveAlias( node, nextGeneratedId++ );
                    if ( shouldExpandIdentifiers() ) {
                        // Since we're expanding identifiers, we should make the aliases explicit too, otherwise the expanded query will not be consistent if we convert back to SQL, e.g.
                        // "select EXPR$1.EXPR$2 from values (1)".
                        newNode = (SqlNode) SqlValidatorUtil.addAlias( node, alias );
                    }
                    break;
            }
        }

        if ( lateral ) {
            SqlValidatorScope s = usingScope;
            while ( s instanceof JoinScope ) {
                s = ((JoinScope) s).getUsingScope();
            }
            final SqlNode node2 = s != null ? s.getNode() : node;
            final TableScope tableScope = new TableScope( parentScope, node2 );
            if ( usingScope instanceof ListScope ) {
                for ( ScopeChild child : ((ListScope) usingScope).children ) {
                    tableScope.addChild( child.namespace, child.name, child.nullable );
                }
            }
            parentScope = tableScope;
        }

        SqlCall call;
        SqlNode operand;
        SqlNode newOperand;

        switch ( kind ) {
            case AS:
                call = (SqlCall) node;
                if ( alias == null ) {
                    alias = call.operand( 1 ).toString();
                }
                final boolean needAlias = call.operandCount() > 2;
                expr = call.operand( 0 );
                newExpr =
                        registerFrom(
                                parentScope,
                                usingScope,
                                !needAlias,
                                expr,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral );
                if ( newExpr != expr ) {
                    call.setOperand( 0, newExpr );
                }

                // If alias has a column list, introduce a namespace to translate column names. We skipped registering it just now.
                if ( needAlias ) {
                    registerNamespace(
                            usingScope,
                            alias,
                            new AliasNamespace( this, call, enclosingNode ),
                            forceNullable );
                }
                return node;
            case MATCH_RECOGNIZE:
                registerMatchRecognize(
                        parentScope,
                        usingScope,
                        (SqlMatchRecognize) node,
                        enclosingNode,
                        alias,
                        forceNullable );
                return node;
            case TABLESAMPLE:
                call = (SqlCall) node;
                expr = call.operand( 0 );
                newExpr =
                        registerFrom(
                                parentScope,
                                usingScope,
                                true,
                                expr,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral );
                if ( newExpr != expr ) {
                    call.setOperand( 0, newExpr );
                }
                return node;

            case JOIN:
                final SqlJoin join = (SqlJoin) node;
                final JoinScope joinScope = new JoinScope( parentScope, usingScope, join );
                scopes.put( join, joinScope );
                final SqlNode left = join.getLeft();
                final SqlNode right = join.getRight();
                final boolean rightIsLateral = isLateral( right );
                boolean forceLeftNullable = forceNullable;
                boolean forceRightNullable = forceNullable;
                switch ( join.getJoinType() ) {
                    case LEFT:
                        forceRightNullable = true;
                        break;
                    case RIGHT:
                        forceLeftNullable = true;
                        break;
                    case FULL:
                        forceLeftNullable = true;
                        forceRightNullable = true;
                        break;
                }
                final SqlNode newLeft =
                        registerFrom(
                                parentScope,
                                joinScope,
                                true,
                                left,
                                left,
                                null,
                                null,
                                forceLeftNullable,
                                lateral );
                if ( newLeft != left ) {
                    join.setLeft( newLeft );
                }
                final SqlNode newRight =
                        registerFrom(
                                parentScope,
                                joinScope,
                                true,
                                right,
                                right,
                                null,
                                null,
                                forceRightNullable,
                                lateral );
                if ( newRight != right ) {
                    join.setRight( newRight );
                }
                registerSubQueries( joinScope, join.getCondition() );
                final JoinNamespace joinNamespace = new JoinNamespace( this, join );
                registerNamespace( null, null, joinNamespace, forceNullable );
                return join;

            case IDENTIFIER:
                final SqlIdentifier id = (SqlIdentifier) node;
                final IdentifierNamespace newNs =
                        new IdentifierNamespace(
                                this,
                                id,
                                extendList,
                                enclosingNode,
                                parentScope );
                registerNamespace(
                        register ? usingScope : null,
                        alias,
                        newNs,
                        forceNullable );
                if ( tableScope == null ) {
                    tableScope = new TableScope( parentScope, node );
                }
                tableScope.addChild( newNs, alias, forceNullable );
                if ( extendList != null && extendList.size() != 0 ) {
                    return enclosingNode;
                }
                return newNode;

            case LATERAL:
                return registerFrom(
                        parentScope,
                        usingScope,
                        register,
                        ((SqlCall) node).operand( 0 ),
                        enclosingNode,
                        alias,
                        extendList,
                        forceNullable,
                        true );

            case COLLECTION_TABLE:
                call = (SqlCall) node;
                operand = call.operand( 0 );
                newOperand =
                        registerFrom(
                                parentScope,
                                usingScope,
                                register,
                                operand,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable, lateral );
                if ( newOperand != operand ) {
                    call.setOperand( 0, newOperand );
                }
                scopes.put( node, parentScope );
                return newNode;

            case UNNEST:
                if ( !lateral ) {
                    return registerFrom(
                            parentScope,
                            usingScope,
                            register,
                            node,
                            enclosingNode,
                            alias,
                            extendList,
                            forceNullable,
                            true );
                }
                // fall through
            case SELECT:
            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case OTHER_FUNCTION:
                if ( alias == null ) {
                    alias = deriveAlias( node, nextGeneratedId++ );
                }
                registerQuery(
                        parentScope,
                        register ? usingScope : null,
                        node,
                        enclosingNode,
                        alias,
                        forceNullable );
                return newNode;

            case OVER:
                if ( !shouldAllowOverRelation() ) {
                    throw Util.unexpected( kind );
                }
                call = (SqlCall) node;
                final OverScope overScope = new OverScope( usingScope, call );
                scopes.put( call, overScope );
                operand = call.operand( 0 );
                newOperand =
                        registerFrom(
                                parentScope,
                                overScope,
                                true,
                                operand,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral );
                if ( newOperand != operand ) {
                    call.setOperand( 0, newOperand );
                }

                for ( ScopeChild child : overScope.children ) {
                    registerNamespace(
                            register ? usingScope : null,
                            child.name,
                            child.namespace,
                            forceNullable );
                }

                return newNode;

            case EXTEND:
                final SqlCall extend = (SqlCall) node;
                return registerFrom(
                        parentScope,
                        usingScope,
                        true,
                        extend.getSqlOperandList().get( 0 ),
                        extend,
                        alias,
                        (SqlNodeList) extend.getOperandList().get( 1 ),
                        forceNullable,
                        lateral );

            default:
                throw Util.unexpected( kind );
        }
    }


    private static boolean isLateral( SqlNode node ) {
        switch ( node.getKind() ) {
            case LATERAL:
            case UNNEST:
                // Per SQL std, UNNEST is implicitly LATERAL.
                return true;
            case AS:
                return isLateral( ((SqlCall) node).operand( 0 ) );
            default:
                return false;
        }
    }


    protected boolean shouldAllowOverRelation() {
        return false;
    }


    /**
     * Creates a namespace for a <code>SELECT</code> node. Derived class may override this factory method.
     *
     * @param select Select node
     * @param enclosingNode Enclosing node
     * @return Select namespace
     */
    protected SelectNamespace createSelectNamespace( SqlSelect select, SqlNode enclosingNode ) {
        return new SelectNamespace( this, select, enclosingNode );
    }


    /**
     * Creates a namespace for a set operation (<code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override this factory method.
     *
     * @param call Call to set operation
     * @param enclosingNode Enclosing node
     * @return Set operation namespace
     */
    protected SetopNamespace createSetopNamespace( SqlCall call, SqlNode enclosingNode ) {
        return new SetopNamespace( this, call, enclosingNode );
    }


    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Query node
     * @param alias Name of this query within its parent. Must be specified if usingScope != null
     */
    private void registerQuery( SqlValidatorScope parentScope, SqlValidatorScope usingScope, SqlNode node, SqlNode enclosingNode, String alias, boolean forceNullable ) {
        Preconditions.checkArgument( usingScope == null || alias != null );
        registerQuery(
                parentScope,
                usingScope,
                node,
                enclosingNode,
                alias,
                forceNullable,
                true );
    }


    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Query node
     * @param alias Name of this query within its parent. Must be specified if usingScope != null
     * @param checkUpdate if true, validate that the update feature is supported if validating the update statement
     */
    private void registerQuery( SqlValidatorScope parentScope, SqlValidatorScope usingScope, SqlNode node, SqlNode enclosingNode, String alias, boolean forceNullable, boolean checkUpdate ) {
        Objects.requireNonNull( node );
        Objects.requireNonNull( enclosingNode );
        Preconditions.checkArgument( usingScope == null || alias != null );

        SqlCall call;
        List<Node> operands;
        switch ( node.getKind() ) {
            case SELECT:
                final SqlSelect select = (SqlSelect) node;
                final SelectNamespace selectNs = createSelectNamespace( select, enclosingNode );
                registerNamespace( usingScope, alias, selectNs, forceNullable );
                final SqlValidatorScope windowParentScope =
                        (usingScope != null)
                                ? usingScope
                                : parentScope;
                SelectScope selectScope = new SelectScope( parentScope, windowParentScope, select );
                scopes.put( select, selectScope );

                // Start by registering the WHERE clause
                whereScopes.put( select, selectScope );
                registerOperandSubQueries(
                        selectScope,
                        select,
                        SqlSelect.WHERE_OPERAND );

                // Register FROM with the inherited scope 'parentScope', not 'selectScope', otherwise tables in the FROM clause would be able to see each other.
                final SqlNode from = select.getSqlFrom();
                if ( from != null ) {
                    final SqlNode newFrom =
                            registerFrom(
                                    parentScope,
                                    selectScope,
                                    true,
                                    from,
                                    from,
                                    null,
                                    null,
                                    false,
                                    false );
                    if ( newFrom != from ) {
                        select.setFrom( newFrom );
                    }
                }

                // If this is an aggregating query, the SELECT list and HAVING clause use a different scope, where you can only reference columns which are in the GROUP BY clause.
                SqlValidatorScope aggScope = selectScope;
                if ( isAggregate( select ) ) {
                    aggScope = new AggregatingSelectScope( selectScope, select, false );
                    selectScopes.put( select, aggScope );
                } else {
                    selectScopes.put( select, selectScope );
                }
                if ( select.getGroup() != null ) {
                    GroupByScope groupByScope = new GroupByScope( selectScope, select.getGroup(), select );
                    groupByScopes.put( select, groupByScope );
                    registerSubQueries( groupByScope, select.getGroup() );
                }
                registerOperandSubQueries(
                        aggScope,
                        select,
                        SqlSelect.HAVING_OPERAND );
                registerSubQueries( aggScope, select.getSqlSelectList() );
                final SqlNodeList orderList = select.getOrderList();
                if ( orderList != null ) {
                    // If the query is 'SELECT DISTINCT', restrict the columns available to the ORDER BY clause.
                    if ( select.isDistinct() ) {
                        aggScope = new AggregatingSelectScope( selectScope, select, true );
                    }
                    OrderByScope orderScope = new OrderByScope( aggScope, orderList, select );
                    orderScopes.put( select, orderScope );
                    registerSubQueries( orderScope, orderList );

                    if ( !isAggregate( select ) ) {
                        // Since this is not an aggregating query, there cannot be any aggregates in the ORDER BY clause.
                        SqlNode agg = aggFinder.findAgg( orderList );
                        if ( agg != null ) {
                            throw newValidationError( agg, RESOURCE.aggregateIllegalInOrderBy() );
                        }
                    }
                }
                break;

            case INTERSECT:
                validateFeature( RESOURCE.sQLFeature_F302(), node.getPos() );
                registerSetop(
                        parentScope,
                        usingScope,
                        node,
                        node,
                        alias,
                        forceNullable );
                break;

            case EXCEPT:
                validateFeature( RESOURCE.sQLFeature_E071_03(), node.getPos() );
                registerSetop(
                        parentScope,
                        usingScope,
                        node,
                        node,
                        alias,
                        forceNullable );
                break;

            case UNION:
                registerSetop(
                        parentScope,
                        usingScope,
                        node,
                        node,
                        alias,
                        forceNullable );
                break;

            case WITH:
                registerWith(
                        parentScope,
                        usingScope,
                        (SqlWith) node,
                        enclosingNode,
                        alias,
                        forceNullable,
                        checkUpdate );
                break;

            case VALUES:
                call = (SqlCall) node;
                scopes.put( call, parentScope );
                final TableConstructorNamespace tableConstructorNamespace =
                        new TableConstructorNamespace(
                                this,
                                call,
                                parentScope,
                                enclosingNode );
                registerNamespace(
                        usingScope,
                        alias,
                        tableConstructorNamespace,
                        forceNullable );
                operands = call.getOperandList();
                for ( int i = 0; i < operands.size(); ++i ) {
                    assert operands.get( i ).getKind() == Kind.ROW;

                    // FIXME jvs 9-Feb-2005:  Correlation should be illegal in these sub-queries.  Same goes for any non-lateral SELECT in the FROM list.
                    registerOperandSubQueries( parentScope, call, i );
                }
                break;

            case INSERT:
                SqlInsert insertCall = (SqlInsert) node;
                InsertNamespace insertNs =
                        new InsertNamespace(
                                this,
                                insertCall,
                                enclosingNode,
                                parentScope );
                registerNamespace(
                        usingScope,
                        null,
                        insertNs,
                        forceNullable );
                registerQuery(
                        parentScope,
                        usingScope,
                        insertCall.getSource(),
                        enclosingNode,
                        null,
                        false );
                break;

            case DELETE:
                SqlDelete deleteCall = (SqlDelete) node;
                DeleteNamespace deleteNs =
                        new DeleteNamespace(
                                this,
                                deleteCall,
                                enclosingNode,
                                parentScope );
                registerNamespace(
                        usingScope,
                        null,
                        deleteNs,
                        forceNullable );
                registerQuery(
                        parentScope,
                        usingScope,
                        deleteCall.getSourceSelect(),
                        enclosingNode,
                        null,
                        false );
                break;

            case UPDATE:
                if ( checkUpdate ) {
                    validateFeature( RESOURCE.sQLFeature_E101_03(), node.getPos() );
                }
                SqlUpdate updateCall = (SqlUpdate) node;
                UpdateNamespace updateNs =
                        new UpdateNamespace(
                                this,
                                updateCall,
                                enclosingNode,
                                parentScope );
                registerNamespace(
                        usingScope,
                        null,
                        updateNs,
                        forceNullable );
                registerQuery(
                        parentScope,
                        usingScope,
                        updateCall.getSourceSelect(),
                        enclosingNode,
                        null,
                        false );
                break;

            case MERGE:
                validateFeature( RESOURCE.sQLFeature_F312(), node.getPos() );
                SqlMerge mergeCall = (SqlMerge) node;
                MergeNamespace mergeNs =
                        new MergeNamespace(
                                this,
                                mergeCall,
                                enclosingNode,
                                parentScope );
                registerNamespace(
                        usingScope,
                        null,
                        mergeNs,
                        forceNullable );
                registerQuery(
                        parentScope,
                        usingScope,
                        mergeCall.getSourceSelect(),
                        enclosingNode,
                        null,
                        false );

                // update call can reference either the source table reference or the target table, so set its parent scope to the merge's source select; when validating the update, skip the feature
                // validation check
                if ( mergeCall.getUpdateCall() != null ) {
                    registerQuery(
                            whereScopes.get( mergeCall.getSourceSelect() ),
                            null,
                            mergeCall.getUpdateCall(),
                            enclosingNode,
                            null,
                            false,
                            false );
                }
                if ( mergeCall.getInsertCall() != null ) {
                    registerQuery(
                            parentScope,
                            null,
                            mergeCall.getInsertCall(),
                            enclosingNode,
                            null,
                            false );
                }
                break;

            case UNNEST:
                call = (SqlCall) node;
                final UnnestNamespace unnestNs =
                        new UnnestNamespace(
                                this,
                                call,
                                parentScope,
                                enclosingNode );
                registerNamespace(
                        usingScope,
                        alias,
                        unnestNs,
                        forceNullable );
                registerOperandSubQueries(
                        parentScope,
                        call,
                        0 );
                scopes.put( node, parentScope );
                break;

            case OTHER_FUNCTION:
                call = (SqlCall) node;
                ProcedureNamespace procNs =
                        new ProcedureNamespace(
                                this,
                                parentScope,
                                call,
                                enclosingNode );
                registerNamespace(
                        usingScope,
                        alias,
                        procNs,
                        forceNullable );
                registerSubQueries( parentScope, call );
                break;

            case MULTISET_QUERY_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
                validateFeature( RESOURCE.sQLFeature_S271(), node.getPos() );
                call = (SqlCall) node;
                CollectScope cs = new CollectScope( parentScope, usingScope, call );
                final CollectNamespace tableConstructorNs = new CollectNamespace( call, cs, enclosingNode );
                final String alias2 = deriveAlias( node, nextGeneratedId++ );
                registerNamespace(
                        usingScope,
                        alias2,
                        tableConstructorNs,
                        forceNullable );
                operands = call.getOperandList();
                for ( int i = 0; i < operands.size(); i++ ) {
                    registerOperandSubQueries( parentScope, call, i );
                }
                break;

            default:
                throw Util.unexpected( node.getKind() );
        }
    }


    private void registerSetop( SqlValidatorScope parentScope, SqlValidatorScope usingScope, SqlNode node, SqlNode enclosingNode, String alias, boolean forceNullable ) {
        SqlCall call = (SqlCall) node;
        final SetopNamespace setopNamespace = createSetopNamespace( call, enclosingNode );
        registerNamespace(
                usingScope,
                alias,
                setopNamespace,
                forceNullable );

        // A setop is in the same scope as its parent.
        scopes.put( call, parentScope );
        for ( Node operand : call.getOperandList() ) {
            registerQuery(
                    parentScope,
                    null,
                    (SqlNode) operand,
                    (SqlNode) operand,
                    null,
                    false );
        }
    }


    private void registerWith( SqlValidatorScope parentScope, SqlValidatorScope usingScope, SqlWith with, SqlNode enclosingNode, String alias, boolean forceNullable, boolean checkUpdate ) {
        final WithNamespace withNamespace = new WithNamespace( this, with, enclosingNode );
        registerNamespace( usingScope, alias, withNamespace, forceNullable );

        SqlValidatorScope scope = parentScope;
        for ( Node withItem_ : with.withList ) {
            final SqlWithItem withItem = (SqlWithItem) withItem_;
            final WithScope withScope = new WithScope( scope, withItem );
            scopes.put( withItem, withScope );

            registerQuery(
                    scope,
                    null,
                    withItem.query,
                    with,
                    withItem.name.getSimple(),
                    false );
            registerNamespace(
                    null,
                    alias,
                    new WithItemNamespace( this, withItem, enclosingNode ),
                    false );
            scope = withScope;
        }

        registerQuery(
                scope,
                null,
                with.body,
                enclosingNode,
                alias,
                forceNullable,
                checkUpdate );
    }


    @Override
    public boolean isAggregate( SqlSelect select ) {
        if ( getAggregate( select ) != null ) {
            return true;
        }
        // Also when nested window aggregates are present
        for ( SqlCall call : overFinder.findAll( select.getSqlSelectList().getSqlList() ) ) {
            assert call.getKind() == Kind.OVER;
            if ( isNestedAggregateWindow( call.operand( 0 ) ) ) {
                return true;
            }
            if ( isOverAggregateWindow( call.operand( 1 ) ) ) {
                return true;
            }
        }
        return false;
    }


    protected boolean isNestedAggregateWindow( SqlNode node ) {
        AggFinder nestedAggFinder = new AggFinder( opTab, false, false, false, aggFinder );
        return nestedAggFinder.findAgg( node ) != null;
    }


    protected boolean isOverAggregateWindow( SqlNode node ) {
        return aggFinder.findAgg( node ) != null;
    }


    /**
     * Returns the parse tree node (GROUP BY, HAVING, or an aggregate function call) that causes {@code select} to be an aggregate query, or null if it is not an aggregate query.
     * <p>
     * The node is useful context for error messages, but you cannot assume that the node is the only aggregate function.
     */
    protected SqlNode getAggregate( SqlSelect select ) {
        SqlNode node = select.getGroup();
        if ( node != null ) {
            return node;
        }
        node = select.getHaving();
        if ( node != null ) {
            return node;
        }
        return getAgg( select );
    }


    /**
     * If there is at least one call to an aggregate function, returns the first.
     */
    private SqlNode getAgg( SqlSelect select ) {
        final SelectScope selectScope = getRawSelectScope( select );
        if ( selectScope != null ) {
            final List<SqlNode> selectList = selectScope.getExpandedSelectList();
            if ( selectList != null ) {
                return aggFinder.findAgg( selectList );
            }
        }
        return aggFinder.findAgg( select.getSqlSelectList() );
    }


    @Override
    public boolean isAggregate( SqlNode selectNode ) {
        return aggFinder.findAgg( selectNode ) != null;
    }


    private void validateNodeFeature( SqlNode node ) {
        if ( Objects.requireNonNull( node.getKind() ) == Kind.MULTISET_VALUE_CONSTRUCTOR ) {
            validateFeature( RESOURCE.sQLFeature_S271(), node.getPos() );
        }
    }


    private void registerSubQueries( SqlValidatorScope parentScope, SqlNode node ) {
        if ( node == null ) {
            return;
        }
        if ( node.getKind().belongsTo( Kind.QUERY )
                || node.getKind() == Kind.MULTISET_QUERY_CONSTRUCTOR
                || node.getKind() == Kind.MULTISET_VALUE_CONSTRUCTOR ) {
            registerQuery( parentScope, null, node, node, null, false );
        } else if ( node instanceof SqlCall call ) {
            validateNodeFeature( node );
            for ( int i = 0; i < call.operandCount(); i++ ) {
                registerOperandSubQueries( parentScope, call, i );
            }
        } else if ( node instanceof SqlNodeList list ) {
            for ( int i = 0, count = list.size(); i < count; i++ ) {
                Node listNode = list.get( i );
                if ( listNode.getKind().belongsTo( Kind.QUERY ) ) {
                    listNode = OperatorRegistry.get( OperatorName.SCALAR_QUERY ).createCall( listNode.getPos(), listNode );
                    list.set( i, listNode );
                }
                registerSubQueries( parentScope, (SqlNode) listNode );
            }
        } else {
            // atomic node -- can be ignored
        }
    }


    /**
     * Registers any sub-queries inside a given call operand, and converts the operand to a scalar sub-query if the operator requires it.
     *
     * @param parentScope Parent scope
     * @param call Call
     * @param operandOrdinal Ordinal of operand within call
     * @see SqlOperator#argumentMustBeScalar(int)
     */
    private void registerOperandSubQueries( SqlValidatorScope parentScope, SqlCall call, int operandOrdinal ) {
        SqlNode operand = call.operand( operandOrdinal );
        if ( operand == null ) {
            return;
        }
        if ( operand.getKind().belongsTo( Kind.QUERY ) && ((SqlOperator) call.getOperator()).argumentMustBeScalar( operandOrdinal ) ) {
            operand = (SqlNode) OperatorRegistry.get( OperatorName.SCALAR_QUERY ).createCall( operand.getPos(), operand );
            call.setOperand( operandOrdinal, operand );
        }
        registerSubQueries( parentScope, operand );
    }


    @Override
    public void validateIdentifier( SqlIdentifier id, SqlValidatorScope scope ) {
        final SqlQualified fqId = scope.fullyQualify( id );
        if ( expandColumnReferences ) {
            // NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its own ideas about qualification.
            id.assignNamesFrom( fqId.identifier );
        } else {
            Util.discard( fqId );
        }
    }


    @Override
    public void validateLiteral( SqlLiteral literal ) {
        switch ( literal.getTypeName() ) {
            case DECIMAL:
                // Decimal and long have the same precision (as 64-bit integers), so the unscaled value of a decimal must fit into a long.

                // REPVIEW jvs:  This should probably be calling over to the available calculator implementations to see what they support.  For now use ESP instead.
                //
                // jhyde: I think the limits should be baked into the type system, not dependent on the calculator implementation.
                PolyBigDecimal bd = (PolyBigDecimal) literal.getValue();
                BigInteger unscaled = bd.bigDecimalValue().unscaledValue();
                long longValue = unscaled.longValue();
                if ( !BigInteger.valueOf( longValue ).equals( unscaled ) ) {
                    // overflow
                    throw newValidationError( literal, RESOURCE.numberLiteralOutOfRange( bd.toString() ) );
                }
                break;

            case DOUBLE:
                validateLiteralAsDouble( literal );
                break;

            case BINARY:
                if ( (literal.getValue().asBinary().getBitCount() % 8) != 0 ) {
                    throw newValidationError( literal, RESOURCE.binaryLiteralOdd() );
                }
                break;

            case DATE:
            case TIME:
            case TIMESTAMP:
                Calendar calendar = literal.getValueAs( Calendar.class );
                final int year = calendar.get( Calendar.YEAR );
                final int era = calendar.get( Calendar.ERA );
                if ( year < 1 || era == GregorianCalendar.BC || year > 9999 ) {
                    throw newValidationError( literal, RESOURCE.dateLiteralOutOfRange( literal.toString() ) );
                }
                break;
            case INTERVAL:
                if ( literal instanceof SqlIntervalLiteral ) {
                    SqlIntervalLiteral.IntervalValue interval = (SqlIntervalLiteral.IntervalValue) literal.getValue();
                    SqlIntervalQualifier intervalQualifier = interval.getIntervalQualifier();

                    // ensure qualifier is good before attempting to validate literal
                    validateIntervalQualifier( intervalQualifier );
                    String intervalStr = interval.getIntervalLiteral();
                    // throws PolyphenyDbContextException if string is invalid
                    int[] values = intervalQualifier.evaluateIntervalLiteral( intervalStr, literal.getPos(), typeFactory.getTypeSystem() );
                    Util.discard( values );
                }
                break;
            default:
                // default is to do nothing
        }
    }


    private void validateLiteralAsDouble( SqlLiteral literal ) {
        double d = literal.getValue().asNumber().doubleValue();
        if ( Double.isInfinite( d ) || Double.isNaN( d ) ) {
            // overflow
            throw newValidationError( literal, RESOURCE.numberLiteralOutOfRange( Util.toScientificNotation( literal.getValue().asBigDecimal().BigDecimalValue() ) ) );
        }

        // REVIEW jvs: what about underflow?
    }


    @Override
    public void validateIntervalQualifier( SqlIntervalQualifier qualifier ) {
        assert qualifier != null;
        boolean startPrecisionOutOfRange = false;
        boolean fractionalSecondPrecisionOutOfRange = false;
        final AlgDataTypeSystem typeSystem = typeFactory.getTypeSystem();

        final int startPrecision = qualifier.getStartPrecision( typeSystem );
        final int fracPrecision = qualifier.getFractionalSecondPrecision( typeSystem );
        final int maxPrecision = typeSystem.getMaxPrecision( qualifier.typeName() );
        final int minPrecision = qualifier.typeName().getMinPrecision();
        final int minScale = qualifier.typeName().getMinScale();
        final int maxScale = typeSystem.getMaxScale( qualifier.typeName() );
        if ( qualifier.isYearMonth() ) {
            if ( startPrecision < minPrecision || startPrecision > maxPrecision ) {
                startPrecisionOutOfRange = true;
            } else {
                if ( fracPrecision < minScale || fracPrecision > maxScale ) {
                    fractionalSecondPrecisionOutOfRange = true;
                }
            }
        } else {
            if ( startPrecision < minPrecision || startPrecision > maxPrecision ) {
                startPrecisionOutOfRange = true;
            } else {
                if ( fracPrecision < minScale || fracPrecision > maxScale ) {
                    fractionalSecondPrecisionOutOfRange = true;
                }
            }
        }

        if ( startPrecisionOutOfRange ) {
            throw newValidationError( qualifier, RESOURCE.intervalStartPrecisionOutOfRange( startPrecision, "INTERVAL " + qualifier ) );
        } else if ( fractionalSecondPrecisionOutOfRange ) {
            throw newValidationError( qualifier, RESOURCE.intervalFractionalSecondPrecisionOutOfRange( fracPrecision, "INTERVAL " + qualifier ) );
        }
    }


    /**
     * Validates the FROM clause of a query, or (recursively) a child node of the FROM clause: AS, OVER, JOIN, VALUES, or sub-query.
     *
     * @param node Node in FROM clause, typically a table or derived table
     * @param targetRowType Desired row type of this expression, or {@link #unknownType} if not fussy. Must not be null.
     * @param scope Scope
     */
    protected void validateFrom( SqlNode node, AlgDataType targetRowType, SqlValidatorScope scope ) {
        Objects.requireNonNull( targetRowType );
        switch ( node.getKind() ) {
            case AS:
                validateFrom(
                        ((SqlCall) node).operand( 0 ),
                        targetRowType,
                        scope );
                break;
            case VALUES:
                validateValues( (SqlCall) node, targetRowType, scope );
                break;
            case JOIN:
                validateJoin( (SqlJoin) node, scope );
                break;
            case OVER:
                validateOver( (SqlCall) node, scope );
                break;
            case UNNEST:
                validateUnnest( (SqlCall) node, scope, targetRowType );
                break;
            default:
                validateQuery( node, scope, targetRowType );
                break;
        }

        // Validate the namespace representation of the node, just in case the validation did not occur implicitly.
        getNamespace( node, scope ).validate( targetRowType );
    }


    protected void validateOver( SqlCall call, SqlValidatorScope scope ) {
        throw new AssertionError( "OVER unexpected in this context" );
    }


    protected void validateUnnest( SqlCall call, SqlValidatorScope scope, AlgDataType targetRowType ) {
        for ( int i = 0; i < call.operandCount(); i++ ) {
            SqlNode expandedItem = expand( call.operand( i ), scope );
            call.setOperand( i, expandedItem );
        }
        validateQuery( call, scope, targetRowType );
    }


    private void checkRollUpInUsing( SqlIdentifier identifier, SqlNode leftOrRight ) {
        leftOrRight = SqlUtil.stripAs( leftOrRight );
        // if it's not a SqlIdentifier then that's fine, it'll be validated somewhere else.
        if ( leftOrRight instanceof SqlIdentifier from ) {
            Entity entity = findEntity(
                    Util.last( from.names )
            );
            String name = Util.last( identifier.names );

            if ( entity != null && entity.isRolledUp( name ) ) {
                throw newValidationError( identifier, RESOURCE.rolledUpNotAllowed( name, "USING" ) );
            }
        }
    }


    protected void validateJoin( SqlJoin join, SqlValidatorScope scope ) {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        boolean natural = join.isNatural();
        final JoinType joinType = join.getJoinType();
        final JoinConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = scopes.get( join );
        validateFrom( left, unknownType, joinScope );
        validateFrom( right, unknownType, joinScope );

        // Validate condition.
        switch ( conditionType ) {
            case NONE:
                Preconditions.checkArgument( condition == null );
                break;
            case ON:
                Preconditions.checkArgument( condition != null );
                SqlNode expandedCondition = expand( condition, joinScope );
                join.setOperand( 5, expandedCondition );
                condition = join.getCondition();
                validateWhereOrOn( joinScope, condition, "ON" );
                checkRollUp( null, join, condition, joinScope, "ON" );
                break;
            case USING:
                SqlNodeList list = (SqlNodeList) condition;

                // Parser ensures that using clause is not empty.
                Preconditions.checkArgument( list.size() > 0, "Empty USING clause" );
                for ( Node node : list ) {
                    SqlIdentifier id = (SqlIdentifier) node;
                    final AlgDataType leftColType = validateUsingCol( id, left );
                    final AlgDataType rightColType = validateUsingCol( id, right );
                    if ( !PolyTypeUtil.isComparable( leftColType, rightColType ) ) {
                        throw newValidationError(
                                id,
                                RESOURCE.naturalOrUsingColumnNotCompatible(
                                        id.getSimple(),
                                        leftColType.toString(),
                                        rightColType.toString() ) );
                    }
                    checkRollUpInUsing( id, left );
                    checkRollUpInUsing( id, right );
                }
                break;
            default:
                throw Util.unexpected( conditionType );
        }

        // Validate NATURAL.
        if ( natural ) {
            if ( condition != null ) {
                throw newValidationError( condition, RESOURCE.naturalDisallowsOnOrUsing() );
            }

            // Join on fields that occur exactly once on each side. Ignore fields that occur more than once on either side.
            final AlgDataType leftRowType = getSqlNamespace( left ).getTupleType();
            final AlgDataType rightRowType = getSqlNamespace( right ).getTupleType();
            final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
            List<String> naturalColumnNames = SqlValidatorUtil.deriveNaturalJoinColumnList( nameMatcher, leftRowType, rightRowType );

            // Check compatibility of the chosen columns.
            for ( String name : naturalColumnNames ) {
                final AlgDataType leftColType = nameMatcher.field( leftRowType, name ).getType();
                final AlgDataType rightColType = nameMatcher.field( rightRowType, name ).getType();
                if ( !PolyTypeUtil.isComparable( leftColType, rightColType ) ) {
                    throw newValidationError( join, RESOURCE.naturalOrUsingColumnNotCompatible( name, leftColType.toString(), rightColType.toString() ) );
                }
            }
        }

        // Which join types require/allow a ON/USING condition, or allow a NATURAL keyword?
        switch ( joinType ) {
            case LEFT_SEMI_JOIN:
                if ( !conformance.isLiberal() ) {
                    throw newValidationError( join.getJoinTypeNode(), RESOURCE.dialectDoesNotSupportFeature( "LEFT SEMI JOIN" ) );
                }
                // fall through
            case INNER:
            case LEFT:
            case RIGHT:
            case FULL:
                if ( (condition == null) && !natural ) {
                    throw newValidationError( join, RESOURCE.joinRequiresCondition() );
                }
                break;
            case COMMA:
            case CROSS:
                if ( condition != null ) {
                    throw newValidationError( join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition() );
                }
                if ( natural ) {
                    throw newValidationError( join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition() );
                }
                break;
            default:
                throw Util.unexpected( joinType );
        }
    }


    /**
     * Throws an error if there is an aggregate or windowed aggregate in the given clause.
     *
     * @param aggFinder Finder for the particular kind(s) of aggregate function
     * @param node Parse tree
     * @param clause Name of clause: "WHERE", "GROUP BY", "ON"
     */
    private void validateNoAggs( AggFinder aggFinder, SqlNode node, String clause ) {
        final SqlCall agg = aggFinder.findAgg( node );
        if ( agg == null ) {
            return;
        }
        final Operator op = agg.getOperator();
        if ( op.getOperatorName() == OperatorName.OVER ) {
            throw newValidationError( agg, RESOURCE.windowedAggregateIllegalInClause( clause ) );
        } else if ( op.isGroup() || op.isGroupAuxiliary() ) {
            throw newValidationError( agg, RESOURCE.groupFunctionMustAppearInGroupByClause( op.getName() ) );
        } else {
            throw newValidationError( agg, RESOURCE.aggregateIllegalInClause( clause ) );
        }
    }


    private AlgDataType validateUsingCol( SqlIdentifier id, SqlNode leftOrRight ) {
        if ( id.names.size() == 1 ) {
            String name = id.names.get( 0 );
            final SqlValidatorNamespace namespace = getSqlNamespace( leftOrRight );
            final AlgDataType rowType = namespace.getTupleType();
            final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
            final AlgDataTypeField field = nameMatcher.field( rowType, name );
            if ( field != null ) {
                if ( nameMatcher.frequency( rowType.getFieldNames(), name ) > 1 ) {
                    throw newValidationError( id, RESOURCE.columnInUsingNotUnique( id.toString() ) );
                }
                return field.getType();
            }
        }
        throw newValidationError( id, RESOURCE.fieldNotFound( id.toString() ) );
    }


    /**
     * Validates a SELECT statement.
     *
     * @param select Select statement
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     */
    protected void validateSelect( SqlSelect select, AlgDataType targetRowType ) {
        assert targetRowType != null;
        // Namespace is either a select namespace or a wrapper around one.
        final SelectNamespace ns = getSqlNamespace( select ).unwrap( SelectNamespace.class );

        // Its rowtype is null, meaning it hasn't been validated yet.
        // This is important, because we need to take the targetRowType into account.
        assert ns.rowType == null;

        if ( select.isDistinct() ) {
            validateFeature( RESOURCE.sQLFeature_E051_01(), select.getModifierNode( SqlSelectKeyword.DISTINCT ).getPos() );
        }

        final SqlNodeList selectItems = select.getSqlSelectList();
        AlgDataType fromType = unknownType;
        if ( selectItems.size() == 1 ) {
            final Node selectItem = selectItems.get( 0 );
            if ( selectItem instanceof SqlIdentifier id ) {
                if ( id.isStar() && (id.names.size() == 1) ) {
                    // Special case: for INSERT ... VALUES(?,?), the SQL standard says we're supposed to propagate the target types down.  So iff the select list is an unqualified
                    // star (as it will be after an INSERT ... VALUES has been expanded), then propagate.
                    fromType = targetRowType;
                }
            }
        }

        // Make sure that items in FROM clause have distinct aliases.
        final SelectScope fromScope = (SelectScope) getFromScope( select );
        List<String> names = fromScope.getChildNames();

        names = names.stream().map( s -> s.toUpperCase( Locale.ROOT ) ).toList();

        final int duplicateAliasOrdinal = Util.firstDuplicate( names );
        if ( duplicateAliasOrdinal >= 0 ) {
            final ScopeChild child = fromScope.children.get( duplicateAliasOrdinal );
            throw newValidationError( child.namespace.getEnclosingNode(), RESOURCE.fromAliasDuplicate( child.name ) );
        }

        if ( select.getSqlFrom() == null ) {
            if ( conformance.isFromRequired() ) {
                throw newValidationError( select, RESOURCE.selectMissingFrom() );
            }
        } else {
            validateFrom( select.getSqlFrom(), fromType, fromScope );
        }

        validateWhereClause( select );
        validateGroupClause( select );
        validateHavingClause( select );
        validateWindowClause( select );
        handleOffsetFetch( select.getOffset(), select.getFetch() );

        // Validate the SELECT clause late, because a select item might depend on the GROUP BY list, or the window function might reference window name in the WINDOW clause etc.
        final AlgDataType rowType = validateSelectList( selectItems, select, targetRowType );
        ns.setType( rowType );

        // Validate ORDER BY after we have set ns.rowType because in some dialects you can refer to columns of the select list, e.g.
        // "SELECT empno AS x FROM emp ORDER BY x"
        validateOrderList( select );

        if ( shouldCheckForRollUp( select.getSqlFrom() ) ) {
            checkRollUpInSelectList( select );
            checkRollUp( null, select, select.getWhere(), getWhereScope( select ) );
            checkRollUp( null, select, select.getHaving(), getHavingScope( select ) );
            checkRollUpInWindowDecl( select );
            checkRollUpInGroupBy( select );
            checkRollUpInOrderBy( select );
        }
    }


    private void checkRollUpInSelectList( SqlSelect select ) {
        SqlValidatorScope scope = getSelectScope( select );
        for ( Node item : select.getSqlSelectList() ) {
            checkRollUp( null, select, (SqlNode) item, scope );
        }
    }


    private void checkRollUpInGroupBy( SqlSelect select ) {
        SqlNodeList group = select.getGroup();
        if ( group != null ) {
            for ( Node node : group ) {
                checkRollUp( null, select, (SqlNode) node, getGroupScope( select ), "GROUP BY" );
            }
        }
    }


    private void checkRollUpInOrderBy( SqlSelect select ) {
        SqlNodeList orderList = select.getOrderList();
        if ( orderList != null ) {
            for ( Node node : orderList ) {
                checkRollUp( null, select, (SqlNode) node, getOrderScope( select ), "ORDER BY" );
            }
        }
    }


    private void checkRollUpInWindow( SqlWindow window, SqlValidatorScope scope ) {
        if ( window != null ) {
            for ( Node node : window.getPartitionList() ) {
                checkRollUp( null, window, (SqlNode) node, scope, "PARTITION BY" );
            }

            for ( Node node : window.getOrderList() ) {
                checkRollUp( null, window, (SqlNode) node, scope, "ORDER BY" );
            }
        }
    }


    private void checkRollUpInWindowDecl( SqlSelect select ) {
        for ( Node decl : select.getWindowList() ) {
            checkRollUpInWindow( (SqlWindow) decl, getSelectScope( select ) );
        }
    }


    private SqlNode stripDot( SqlNode node ) {
        if ( node != null && node.getKind() == Kind.DOT ) {
            return stripDot( ((SqlCall) node).operand( 0 ) );
        }
        return node;
    }


    private void checkRollUp( SqlNode grandParent, SqlNode parent, SqlNode current, SqlValidatorScope scope, String optionalClause ) {
        current = SqlUtil.stripAs( current );
        if ( current instanceof SqlCall && !(current instanceof SqlSelect) ) {
            // Validate OVER separately
            checkRollUpInWindow( getWindowInOver( current ), scope );
            current = stripOver( current );

            List<Node> children = ((SqlCall) stripDot( current )).getOperandList();
            for ( Node child : children ) {
                checkRollUp( parent, current, (SqlNode) child, scope, optionalClause );
            }
        } else if ( current instanceof SqlIdentifier id ) {
            if ( !id.isStar() && isRolledUpColumn( id, scope ) ) {
                if ( !isAggregation( parent.getKind() ) || !isRolledUpColumnAllowedInAgg( id, scope, (SqlCall) parent, grandParent ) ) {
                    String context = optionalClause != null ? optionalClause : parent.getKind().toString();
                    throw newValidationError( id, RESOURCE.rolledUpNotAllowed( deriveAlias( id, 0 ), context ) );
                }
            }
        }
    }


    private void checkRollUp( SqlNode grandParent, SqlNode parent, SqlNode current, SqlValidatorScope scope ) {
        checkRollUp( grandParent, parent, current, scope, null );
    }


    private SqlWindow getWindowInOver( SqlNode over ) {
        if ( over.getKind() == Kind.OVER ) {
            Node window = ((SqlCall) over).getOperandList().get( 1 );
            if ( window instanceof SqlWindow ) {
                return (SqlWindow) window;
            }
            // SqlIdentifier, gets validated elsewhere
            return null;
        }
        return null;
    }


    private static SqlNode stripOver( SqlNode node ) {
        if ( Objects.requireNonNull( node.getKind() ) == Kind.OVER ) {
            return (SqlNode) ((SqlCall) node).getOperandList().get( 0 );
        }
        return node;
    }


    private Pair<String, String> findTableColumnPair( SqlIdentifier identifier, SqlValidatorScope scope ) {
        SqlCall call = SqlUtil.makeCall( getOperatorTable(), identifier );
        if ( call != null ) {
            return null;
        }
        SqlQualified qualified = scope.fullyQualify( identifier );

        List<String> names = qualified.identifier.names;

        if ( names.size() < 2 ) {
            return null;
        }

        return new Pair<>( names.get( names.size() - 2 ), Util.last( names ) );
    }


    // Returns true iff the given column is valid inside the given aggCall.
    private boolean isRolledUpColumnAllowedInAgg( SqlIdentifier identifier, SqlValidatorScope scope, SqlCall aggCall, SqlNode parent ) {
        Pair<String, String> pair = findTableColumnPair( identifier, scope );

        if ( pair == null ) {
            return true;
        }

        String tableAlias = pair.left;

        Entity entity = findEntity( tableAlias );
        if ( entity != null ) {
            return entity.rolledUpColumnValidInsideAgg();
        }
        return true;
    }


    // Returns true iff the given column is actually rolled up.
    private boolean isRolledUpColumn( SqlIdentifier identifier, SqlValidatorScope scope ) {
        Pair<String, String> pair = findTableColumnPair( identifier, scope );

        if ( pair == null ) {
            return false;
        }

        String tableAlias = pair.left;
        String columnName = pair.right;

        Entity entity = findEntity( tableAlias );
        if ( entity != null ) {
            return entity.isRolledUp( columnName );
        }
        return false;
    }


    /**
     * Given an entity alias, find the corresponding {@link org.polypheny.db.schema.Entity} associated with it
     */
    @Nullable
    private Entity findEntity( String tableName ) {
        return snapshot.rel().getTable( Catalog.defaultNamespaceId, tableName ).orElse( null );
    }


    private boolean shouldCheckForRollUp( SqlNode from ) {
        if ( from != null ) {
            Kind kind = SqlUtil.stripAs( from ).getKind();
            return kind != Kind.VALUES && kind != Kind.SELECT;
        }
        return false;
    }


    /**
     * Validates that a query can deliver the modality it promises. Only called on the top-most SELECT or set operator in the tree.
     */
    private void validateModality( SqlNode query ) {
        final Modality modality = deduceModality( query );
        if ( query instanceof SqlSelect select ) {
            validateModality( select, modality, true );
        } else if ( query.getKind() == Kind.VALUES ) {
            if ( Objects.requireNonNull( modality ) == Modality.STREAM ) {
                throw newValidationError( query, RESOURCE.cannotStreamValues() );
            }
        } else {
            assert query.isA( Kind.SET_QUERY );
            final SqlCall call = (SqlCall) query;
            for ( Node operand : call.getOperandList() ) {
                if ( deduceModality( (SqlNode) operand ) != modality ) {
                    throw newValidationError(
                            operand,
                            Static.RESOURCE.streamSetOpInconsistentInputs() );
                }
                validateModality( (SqlNode) operand );
            }
        }
    }


    /**
     * Return the intended modality of a SELECT or set-op.
     */
    private Modality deduceModality( SqlNode query ) {
        if ( query instanceof SqlSelect select ) {
            return select.getModifierNode( SqlSelectKeyword.STREAM ) != null
                    ? Modality.STREAM
                    : Modality.RELATION;
        } else if ( query.getKind() == Kind.VALUES ) {
            return Modality.RELATION;
        } else {
            assert query.isA( Kind.SET_QUERY );
            final SqlCall call = (SqlCall) query;
            return deduceModality( (SqlNode) call.getOperandList().get( 0 ) );
        }
    }


    @Override
    public boolean validateModality( SqlSelect select, Modality modality, boolean fail ) {
        final SelectScope scope = getRawSelectScope( select );

        if ( Objects.requireNonNull( modality ) == Modality.STREAM ) {
            if ( scope.children.size() == 1 ) {
                for ( ScopeChild child : scope.children ) {
                    if ( !child.namespace.supportsModality( modality ) ) {
                        if ( fail ) {
                            throw newValidationError( child.namespace.getNode(), RESOURCE.cannotConvertToStream( child.name ) );
                        } else {
                            return false;
                        }
                    }
                }
            } else {
                int supportsModalityCount = 0;
                for ( ScopeChild child : scope.children ) {
                    if ( child.namespace.supportsModality( modality ) ) {
                        ++supportsModalityCount;
                    }
                }

                if ( supportsModalityCount == 0 ) {
                    if ( fail ) {
                        String inputs = String.join( ", ", scope.getChildNames() );
                        throw newValidationError( select, RESOURCE.cannotStreamResultsForNonStreamingInputs( inputs ) );
                    } else {
                        return false;
                    }
                }
            }
        } else {
            for ( ScopeChild child : scope.children ) {
                if ( !child.namespace.supportsModality( modality ) ) {
                    if ( fail ) {
                        throw newValidationError( child.namespace.getNode(), RESOURCE.cannotConvertToRelation( child.name ) );
                    } else {
                        return false;
                    }
                }
            }
        }

        // Make sure that aggregation is possible.
        final SqlNode aggregateNode = getAggregate( select );
        if ( aggregateNode != null ) {
            if ( modality == Modality.STREAM ) {
                SqlNodeList groupList = select.getGroup();
                if ( groupList == null || !SqlValidatorUtil.containsMonotonic( scope, groupList ) ) {
                    if ( fail ) {
                        throw newValidationError( aggregateNode, RESOURCE.streamMustGroupByMonotonic() );
                    } else {
                        return false;
                    }
                }
            }
        }

        // Make sure that ORDER BY is possible.
        final SqlNodeList orderList = select.getOrderList();
        if ( orderList != null && orderList.size() > 0 ) {
            if ( modality == Modality.STREAM ) {
                if ( !hasSortedPrefix( scope, orderList ) ) {
                    if ( fail ) {
                        throw newValidationError( orderList.get( 0 ), RESOURCE.streamMustOrderByMonotonic() );
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }


    /**
     * Returns whether the prefix is sorted.
     */
    private boolean hasSortedPrefix( SelectScope scope, SqlNodeList orderList ) {
        return isSortCompatible( scope, (SqlNode) orderList.get( 0 ), false );
    }


    private boolean isSortCompatible( SelectScope scope, SqlNode node, boolean descending ) {
        if ( Objects.requireNonNull( node.getKind() ) == Kind.DESCENDING ) {
            return isSortCompatible( scope, (SqlNode) ((SqlCall) node).getOperandList().get( 0 ), true );
        }
        final Monotonicity monotonicity = scope.getMonotonicity( node );
        return switch ( monotonicity ) {
            case INCREASING, STRICTLY_INCREASING -> !descending;
            case DECREASING, STRICTLY_DECREASING -> descending;
            default -> false;
        };
    }


    protected void validateWindowClause( SqlSelect select ) {
        final SqlNodeList windowList = select.getWindowList();
        @SuppressWarnings("unchecked") final List<SqlWindow> windows = (List) windowList.getSqlList();
        if ( windows.isEmpty() ) {
            return;
        }

        final SelectScope windowScope = (SelectScope) getFromScope( select );
        assert windowScope != null;

        // 1. ensure window names are simple
        // 2. ensure they are unique within this scope
        for ( SqlWindow window : windows ) {
            SqlIdentifier declName = window.getDeclName();
            if ( !declName.isSimple() ) {
                throw newValidationError( declName, RESOURCE.windowNameMustBeSimple() );
            }

            if ( windowScope.existingWindowName( declName.toString() ) ) {
                throw newValidationError( declName, RESOURCE.duplicateWindowName() );
            } else {
                windowScope.addWindowName( declName.toString() );
            }
        }

        // 7.10 rule 2
        // Check for pairs of windows which are equivalent.
        for ( int i = 0; i < windows.size(); i++ ) {
            SqlNode window1 = windows.get( i );
            for ( int j = i + 1; j < windows.size(); j++ ) {
                SqlNode window2 = windows.get( j );
                if ( window1.equalsDeep( window2, Litmus.IGNORE ) ) {
                    throw newValidationError( window2, RESOURCE.dupWindowSpec() );
                }
            }
        }

        for ( SqlWindow window : windows ) {
            final SqlNodeList expandedOrderList = (SqlNodeList) expand( window.getOrderList(), windowScope );
            window.setOrderList( expandedOrderList );
            expandedOrderList.validate( this, windowScope );

            final SqlNodeList expandedPartitionList = (SqlNodeList) expand( window.getPartitionList(), windowScope );
            window.setPartitionList( expandedPartitionList );
            expandedPartitionList.validate( this, windowScope );
        }

        // Hand off to validate window spec components
        windowList.validate( this, windowScope );
    }


    @Override
    public void validateWith( SqlWith with, SqlValidatorScope scope ) {
        final SqlValidatorNamespace namespace = getSqlNamespace( with );
        validateNamespace( namespace, unknownType );
    }


    @Override
    public void validateWithItem( SqlWithItem withItem ) {
        if ( withItem.columnList != null ) {
            final AlgDataType rowType = getValidatedNodeType( withItem.query );
            final int fieldCount = rowType.getFieldCount();
            if ( withItem.columnList.size() != fieldCount ) {
                throw newValidationError( withItem.columnList, RESOURCE.columnCountMismatch() );
            }
            SqlValidatorUtil.checkIdentifierListForDuplicates( withItem.columnList.getSqlList(), validationErrorFunction );
        } else {
            // Luckily, field names have not been make unique yet.
            final List<String> fieldNames = getValidatedNodeType( withItem.query ).getFieldNames();
            final int i = Util.firstDuplicate( fieldNames );
            if ( i >= 0 ) {
                throw newValidationError( withItem.query, RESOURCE.duplicateColumnAndNoColumnList( fieldNames.get( i ) ) );
            }
        }
    }


    @Override
    public void validateSequenceValue( SqlValidatorScope scope, SqlIdentifier id ) {
        // Resolve identifier as a table.
        final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
        scope.resolveEntity( id.names, SqlValidatorScope.Path.EMPTY, resolved );
        if ( resolved.count() != 1 ) {
            throw newValidationError( id, RESOURCE.entityNameNotFound( id.toString() ) );
        }

        throw newValidationError( id, RESOURCE.notASequence( id.toString() ) );
    }


    @Override
    public SqlValidatorScope getWithScope( SqlNode withItem ) {
        assert withItem.getKind() == Kind.WITH_ITEM;
        return scopes.get( withItem );
    }


    /**
     * Validates the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     */
    protected void validateOrderList( SqlSelect select ) {
        // ORDER BY is validated in a scope where aliases in the SELECT clause are visible. For example, "SELECT empno AS x FROM emp ORDER BY x" is valid.
        SqlNodeList orderList = select.getOrderList();
        if ( orderList == null ) {
            return;
        }
        if ( !shouldAllowIntermediateOrderBy() ) {
            if ( !cursorSet.contains( select ) ) {
                throw newValidationError( select, RESOURCE.invalidOrderByPos() );
            }
        }
        final SqlValidatorScope orderScope = getOrderScope( select );
        Objects.requireNonNull( orderScope );

        List<SqlNode> expandList = new ArrayList<>();
        for ( Node orderItem : orderList ) {
            SqlNode expandedOrderItem = expand( (SqlNode) orderItem, orderScope );
            expandList.add( expandedOrderItem );
        }

        SqlNodeList expandedOrderList = new SqlNodeList( expandList, orderList.getPos() );
        select.setOrderBy( expandedOrderList );

        for ( Node orderItem : expandedOrderList ) {
            validateOrderItem( select, (SqlNode) orderItem );
        }
    }


    /**
     * Validates an item in the GROUP BY clause of a SELECT statement.
     *
     * @param select Select statement
     * @param groupByItem GROUP BY clause item
     */
    private void validateGroupByItem( SqlSelect select, SqlNode groupByItem ) {
        final SqlValidatorScope groupByScope = getGroupScope( select );
        groupByScope.validateExpr( groupByItem );
    }


    /**
     * Validates an item in the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     * @param orderItem ORDER BY clause item
     */
    private void validateOrderItem( SqlSelect select, SqlNode orderItem ) {
        if ( Objects.requireNonNull( orderItem.getKind() ) == Kind.DESCENDING ) {
            validateFeature( RESOURCE.sQLConformance_OrderByDesc(), orderItem.getPos() );
            validateOrderItem( select, ((SqlCall) orderItem).operand( 0 ) );
            return;
        }

        final SqlValidatorScope orderScope = getOrderScope( select );
        validateExpr( orderItem, orderScope );
    }


    @Override
    public SqlNode expandOrderExpr( SqlSelect select, SqlNode orderExpr ) {
        final SqlNode newSqlNode = new OrderExpressionExpander( select, orderExpr ).go();
        if ( newSqlNode != orderExpr ) {
            final SqlValidatorScope scope = getOrderScope( select );
            inferUnknownTypes( unknownType, scope, newSqlNode );
            final AlgDataType type = deriveType( scope, newSqlNode );
            setValidatedNodeType( newSqlNode, type );
        }
        return newSqlNode;
    }


    /**
     * Validates the GROUP BY clause of a SELECT statement. This method is called even if no GROUP BY clause is present.
     */
    protected void validateGroupClause( SqlSelect select ) {
        SqlNodeList groupList = select.getGroup();
        if ( groupList == null ) {
            return;
        }
        final String clause = "GROUP BY";
        validateNoAggs( aggOrOverFinder, groupList, clause );
        final SqlValidatorScope groupScope = getGroupScope( select );
        inferUnknownTypes( unknownType, groupScope, groupList );

        // expand the expression in group list.
        List<SqlNode> expandedList = new ArrayList<>();
        for ( Node groupItem : groupList ) {
            SqlNode expandedItem = expandGroupByOrHavingExpr( (SqlNode) groupItem, groupScope, select, false );
            expandedList.add( expandedItem );
        }
        groupList = new SqlNodeList( expandedList, groupList.getPos() );
        select.setGroupBy( groupList );
        for ( SqlNode groupItem : expandedList ) {
            validateGroupByItem( select, groupItem );
        }

        // Nodes in the GROUP BY clause are expressions except if they are calls to the GROUPING SETS, ROLLUP or CUBE operators; this operators are not
        // expressions, because they do not have a type.
        for ( Node node : groupList ) {
            switch ( node.getKind() ) {
                case GROUPING_SETS:
                case ROLLUP:
                case CUBE:
                    ((SqlNode) node).validate( this, groupScope );
                    break;
                default:
                    ((SqlNode) node).validateExpr( this, groupScope );
            }
        }

        // Derive the type of each GROUP BY item. We don't need the type, but it resolves functions, and that is necessary for deducing monotonicity.
        final SqlValidatorScope selectScope = getSelectScope( select );
        AggregatingSelectScope aggregatingScope = null;
        if ( selectScope instanceof AggregatingSelectScope ) {
            aggregatingScope = (AggregatingSelectScope) selectScope;
        }
        for ( Node groupItem : groupList ) {
            if ( groupItem instanceof SqlNodeList && ((SqlNodeList) groupItem).size() == 0 ) {
                continue;
            }
            validateGroupItem( groupScope, aggregatingScope, (SqlNode) groupItem );
        }

        SqlNode agg = aggFinder.findAgg( groupList );
        if ( agg != null ) {
            throw newValidationError( agg, RESOURCE.aggregateIllegalInClause( clause ) );
        }
    }


    private void validateGroupItem( SqlValidatorScope groupScope, AggregatingSelectScope aggregatingScope, SqlNode groupItem ) {
        switch ( groupItem.getKind() ) {
            case GROUPING_SETS:
            case ROLLUP:
            case CUBE:
                validateGroupingSets( groupScope, aggregatingScope, (SqlCall) groupItem );
                break;
            default:
                if ( groupItem instanceof SqlNodeList ) {
                    break;
                }
                final AlgDataType type = deriveType( groupScope, groupItem );
                setValidatedNodeType( groupItem, type );
        }
    }


    private void validateGroupingSets( SqlValidatorScope groupScope, AggregatingSelectScope aggregatingScope, SqlCall groupItem ) {
        for ( Node node : groupItem.getOperandList() ) {
            validateGroupItem( groupScope, aggregatingScope, (SqlNode) node );
        }
    }


    protected void validateWhereClause( SqlSelect select ) {
        // validate WHERE clause
        final SqlNode where = select.getWhere();
        if ( where == null ) {
            return;
        }
        final SqlValidatorScope whereScope = getWhereScope( select );
        final SqlNode expandedWhere = expand( where, whereScope );
        select.setWhere( expandedWhere );
        validateWhereOrOn( whereScope, expandedWhere, "WHERE" );
    }


    protected void validateWhereOrOn( SqlValidatorScope scope, SqlNode condition, String clause ) {
        validateNoAggs( aggOrOverOrGroupFinder, condition, clause );
        inferUnknownTypes( booleanType, scope, condition );
        condition.validate( this, scope );

        final AlgDataType type = deriveType( scope, condition );
        if ( !PolyTypeUtil.inBooleanFamily( type ) ) {
            throw newValidationError( condition, RESOURCE.condMustBeBoolean( clause ) );
        }
    }


    protected void validateHavingClause( SqlSelect select ) {
        // HAVING is validated in the scope after groups have been created.
        // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY deptno HAVING empno = 10", the reference to 'empno' in the HAVING clause is illegal.
        SqlNode having = select.getHaving();
        if ( having == null ) {
            return;
        }
        final AggregatingScope havingScope = (AggregatingScope) getSelectScope( select );
        if ( getConformance().isHavingAlias() ) {
            SqlNode newExpr = expandGroupByOrHavingExpr( having, havingScope, select, true );
            if ( having != newExpr ) {
                having = newExpr;
                select.setHaving( newExpr );
            }
        }
        havingScope.checkAggregateExpr( having, true );
        inferUnknownTypes( booleanType, havingScope, having );
        having.validate( this, havingScope );
        final AlgDataType type = deriveType( havingScope, having );
        if ( !PolyTypeUtil.inBooleanFamily( type ) ) {
            throw newValidationError( having, RESOURCE.havingMustBeBoolean() );
        }
    }


    protected AlgDataType validateSelectList( final SqlNodeList selectItems, SqlSelect select, AlgDataType targetRowType ) {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items are ignored.

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        final SqlValidatorScope selectScope = getSelectScope( select );
        final List<SqlNode> expandedSelectItems = new ArrayList<>();
        final Set<String> aliases = new HashSet<>();
        final List<AlgDataTypeField> fieldList = new ArrayList<>();

        for ( int i = 0; i < selectItems.size(); i++ ) {
            Node selectItem = selectItems.get( i );
            if ( selectItem instanceof SqlSelect ) {
                handleScalarSubQuery(
                        select,
                        (SqlSelect) selectItem,
                        expandedSelectItems,
                        aliases,
                        fieldList );
            } else {
                expandSelectItem(
                        (SqlNode) selectItem,
                        select,
                        targetRowType.isStruct() && targetRowType.getFieldCount() >= i
                                ? targetRowType.getFields().get( i ).getType()
                                : unknownType,
                        expandedSelectItems,
                        aliases,
                        fieldList,
                        false );
            }
        }

        // Create the new select list with expanded items.  Pass through the original parser position so that any overall failures can still reference the original input text.
        SqlNodeList newSelectList = new SqlNodeList( expandedSelectItems, selectItems.getPos() );
        if ( shouldExpandIdentifiers() ) {
            select.setSelectList( newSelectList );
        }
        getRawSelectScope( select ).setExpandedSelectList( expandedSelectItems );

        // TODO: when SELECT appears as a value sub-query, should be using something other than unknownType for targetRowType
        inferUnknownTypes( targetRowType, selectScope, newSelectList );

        for ( SqlNode selectItem : expandedSelectItems ) {
            validateNoAggs( groupFinder, selectItem, "SELECT" );
            validateExpr( selectItem, selectScope );
        }

        return typeFactory.createStructType( fieldList );
    }


    /**
     * Validates an expression.
     *
     * @param expr Expression
     * @param scope Scope in which expression occurs
     */
    private void validateExpr( SqlNode expr, SqlValidatorScope scope ) {
        if ( expr instanceof SqlCall ) {
            final SqlOperator op = (SqlOperator) ((SqlCall) expr).getOperator();
            if ( op.isAggregator() && op.requiresOver() ) {
                throw newValidationError( expr, RESOURCE.absentOverClause() );
            }
        }

        // Call on the expression to validate itself.
        expr.validateExpr( this, scope );

        // Perform any validation specific to the scope. For example, an aggregating scope requires that expressions are valid aggregations.
        scope.validateExpr( expr );
    }


    /**
     * Processes SubQuery found in Select list. Checks that is actually Scalar sub-query and makes proper entries in each of the 3 lists used to create the final rowType entry.
     *
     * @param parentSelect base SqlSelect item
     * @param selectItem child SqlSelect from select list
     * @param expandedSelectItems Select items after processing
     * @param aliasList built from user or system values
     * @param fieldList Built up entries for each select list entry
     */
    private void handleScalarSubQuery( SqlSelect parentSelect, SqlSelect selectItem, List<SqlNode> expandedSelectItems, Set<String> aliasList, List<AlgDataTypeField> fieldList ) {
        // A scalar sub-query only has one output column.
        if ( 1 != selectItem.getSqlSelectList().size() ) {
            throw newValidationError( selectItem, RESOURCE.onlyScalarSubQueryAllowed() );
        }

        // No expansion in this routine just append to list.
        expandedSelectItems.add( selectItem );

        // Get or generate alias and add to list.
        final String alias = deriveAlias( selectItem, aliasList.size() );
        aliasList.add( alias );

        final SelectScope scope = (SelectScope) getWhereScope( parentSelect );
        final AlgDataType type = deriveType( scope, selectItem );
        setValidatedNodeType( selectItem, type );

        // we do not want to pass on the RelRecordType returned by the sub query.
        // Just the type of the single expression in the sub-query select list.
        assert type instanceof AlgRecordType;
        AlgRecordType rec = (AlgRecordType) type;

        AlgDataType nodeType = rec.getFields().get( 0 ).getType();
        nodeType = typeFactory.createTypeWithNullability( nodeType, true );
        fieldList.add( new AlgDataTypeFieldImpl( 1L, alias, 0, nodeType ) );
    }


    protected AlgDataType createTargetRowType( Entity table, SqlNodeList targetColumnList, boolean append ) {
        return createTargetRowType( table, targetColumnList, append, false );
    }


    /**
     * Derives a row-type for INSERT and UPDATE operations.
     *
     * @param table Target table for INSERT/UPDATE
     * @param targetColumnList List of target columns, or null if not specified
     * @param append Whether to append fields to those in <code>baseRowType</code>
     * @return Rowtype
     */
    protected AlgDataType createTargetRowType( Entity table, SqlNodeList targetColumnList, boolean append, boolean allowDynamic ) {
        AlgDataType baseRowType = table.getTupleType();
        if ( targetColumnList == null ) {
            return baseRowType;
        }
        List<AlgDataTypeField> targetFields = baseRowType.getFields();
        final List<AlgDataTypeField> fields = new ArrayList<>();
        if ( append ) {
            for ( AlgDataTypeField targetField : targetFields ) {
                fields.add( new AlgDataTypeFieldImpl( 1L, CoreUtil.deriveAliasFromOrdinal( fields.size() ), fields.size(), targetField.getType() ) );
            }
        }
        final Set<Integer> assignedFields = new HashSet<>();
        for ( Node node : targetColumnList ) {
            SqlIdentifier id = (SqlIdentifier) node;
            AlgDataTypeField targetField =
                    SqlValidatorUtil.getTargetField(
                            baseRowType,
                            typeFactory,
                            id,
                            snapshot,
                            table,
                            allowDynamic );

            if ( targetField == null ) {

                throw newValidationError( id, RESOURCE.unknownTargetColumn( id.toString() ) );

            }

            if ( !allowDynamic && !assignedFields.add( targetField.getIndex() ) ) {
                throw newValidationError( id, RESOURCE.duplicateTargetColumn( targetField.getName() ) );
            }
            fields.add( targetField );
        }
        return typeFactory.createStructType( fields );
    }


    @Override
    public void validateInsert( SqlInsert insert ) {
        final SqlValidatorNamespace targetNamespace = getSqlNamespace( insert );
        validateNamespace( targetNamespace, unknownType );
        final Entity table =
                SqlValidatorUtil.getLogicalEntity(
                        targetNamespace,
                        snapshot,
                        null,
                        null );

        boolean allowDynamic = insert.getSchemaType() == DataModel.DOCUMENT;

        // INSERT has an optional column name list.  If present then reduce the rowtype to the columns specified.  If not present then the entire target rowtype is used.
        final AlgDataType targetRowType = createTargetRowType( table, insert.getTargetColumnList(), false, allowDynamic );

        final SqlNode source = insert.getSource();
        if ( source instanceof SqlSelect sqlSelect ) {
            validateSelect( sqlSelect, targetRowType );
        } else {
            final SqlValidatorScope scope = scopes.get( source );
            validateQuery( source, scope, targetRowType );
        }

        // REVIEW jvs: In FRG-365, this namespace row type is discarding the type inferred by inferUnknownTypes (which was invoked from validateSelect above).
        // It would be better if that information were used here so that we never saw any untyped nulls during checkTypeAssignment.
        final AlgDataType sourceRowType = getSqlNamespace( source ).getTupleType();
        final AlgDataType logicalTargetRowType = getLogicalTargetRowType( targetRowType, insert );
        setValidatedNodeType( insert, logicalTargetRowType );
        final AlgDataType logicalSourceRowType = getLogicalSourceRowType( sourceRowType, insert );

        checkFieldCount(
                insert.getTargetTable(),
                table,
                source,
                logicalSourceRowType,
                logicalTargetRowType );

        checkTypeAssignment(
                logicalSourceRowType,
                logicalTargetRowType,
                insert );

        validateAccess(
                insert.getTargetTable(),
                table,
                AccessEnum.INSERT );
    }


    private void checkFieldCount( SqlNode node, Entity table, SqlNode source, AlgDataType logicalSourceRowType, AlgDataType logicalTargetRowType ) {
        final int sourceFieldCount = logicalSourceRowType.getFieldCount();
        final int targetFieldCount = logicalTargetRowType.getFieldCount();
        if ( sourceFieldCount != targetFieldCount ) {
            throw newValidationError( node, RESOURCE.unmatchInsertColumn( targetFieldCount, sourceFieldCount ) );
        }
        // Ensure that non-nullable fields are targeted.
        final List<ColumnStrategy> strategies = table.unwrap( LogicalTable.class ).orElseThrow().getColumnStrategies();
        for ( final AlgDataTypeField field : table.getTupleType().getFields() ) {
            final AlgDataTypeField targetField = logicalTargetRowType.getField( field.getName(), true, false );
            switch ( strategies.get( field.getIndex() ) ) {
                case NOT_NULLABLE:
                    assert !field.getType().isNullable();
                    if ( targetField == null ) {
                        throw newValidationError( node, RESOURCE.columnNotNullable( field.getName() ) );
                    }
                    break;
                case NULLABLE:
                    assert field.getType().isNullable();
                    break;
                case VIRTUAL:
                case STORED:
                    if ( targetField != null && !isValuesWithDefault( source, targetField.getIndex() ) ) {
                        throw newValidationError( node, RESOURCE.insertIntoAlwaysGenerated( field.getName() ) );
                    }
            }
        }
    }


    /**
     * Returns whether a query uses {@code DEFAULT} to populate a given column.
     */
    private boolean isValuesWithDefault( SqlNode source, int column ) {
        if ( Objects.requireNonNull( source.getKind() ) == Kind.VALUES ) {
            for ( Node operand : ((SqlCall) source).getOperandList() ) {
                if ( !isRowWithDefault( (SqlNode) operand, column ) ) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }


    private boolean isRowWithDefault( SqlNode operand, int column ) {
        if ( Objects.requireNonNull( operand.getKind() ) == Kind.ROW ) {
            final SqlCall row = (SqlCall) operand;
            return row.getOperandList().size() >= column && row.getOperandList().get( column ).getKind() == Kind.DEFAULT;
        }
        return false;
    }


    protected AlgDataType getLogicalTargetRowType( AlgDataType targetRowType, SqlInsert insert ) {
        if ( insert.getTargetColumnList() == null && conformance.isInsertSubsetColumnsAllowed() ) {
            // Target an implicit subset of columns.
            final SqlNode source = insert.getSource();
            final AlgDataType sourceRowType = getSqlNamespace( source ).getTupleType();
            final AlgDataType logicalSourceRowType = getLogicalSourceRowType( sourceRowType, insert );
            final AlgDataType implicitTargetRowType = typeFactory.createStructType( targetRowType.getFields().subList( 0, logicalSourceRowType.getFieldCount() ) );
            final SqlValidatorNamespace targetNamespace = getSqlNamespace( insert );
            validateNamespace( targetNamespace, implicitTargetRowType );
            return implicitTargetRowType;
        } else {
            // Either the set of columns are explicitly targeted, or target the full set of columns.
            return targetRowType;
        }
    }


    protected AlgDataType getLogicalSourceRowType( AlgDataType sourceRowType, SqlInsert insert ) {
        return sourceRowType;
    }


    protected void checkTypeAssignment( AlgDataType sourceRowType, AlgDataType targetRowType, final SqlNode query ) {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets representing system-maintained columns, so stop after all sources matched
        List<AlgDataTypeField> sourceFields = sourceRowType.getFields();
        List<AlgDataTypeField> targetFields = targetRowType.getFields();
        final int sourceCount = sourceFields.size();
        for ( int i = 0; i < sourceCount; ++i ) {
            AlgDataType sourceType = sourceFields.get( i ).getType();
            AlgDataType targetType = targetFields.get( i ).getType();

            if ( targetType instanceof ArrayType ) {
                if ( sourceType instanceof ArrayType ) {
                    long targetDimension = ((ArrayType) targetType).getDimension();
                    long sourceDimension = ((ArrayType) sourceType).getMaxDimension();
                    if ( sourceDimension > targetDimension && targetDimension > -1 ) {
                        throw newValidationError( query, RESOURCE.exceededDimension( targetFields.get( i ).getName(), sourceDimension, targetDimension ) );
                    }

                    long targetCardinality = ((ArrayType) targetType).getCardinality();
                    long sourceCardinality = ((ArrayType) sourceType).getMaxCardinality();
                    if ( sourceCardinality > targetCardinality && targetCardinality > -1 ) {
                        throw newValidationError( query, RESOURCE.exceededCardinality( targetFields.get( i ).getName(), sourceCardinality, targetCardinality ) );
                    }
                }
            }
            if ( !PolyTypeUtil.canAssignFrom( targetType, sourceType ) ) {
                // FRG-255:  account for UPDATE rewrite; there's probably a better way to do this.
                int iAdjusted = i;
                if ( query instanceof SqlUpdate ) {
                    int nUpdateColumns = ((SqlUpdate) query).getTargetColumnList().size();
                    assert sourceFields.size() >= nUpdateColumns;
                    iAdjusted -= sourceFields.size() - nUpdateColumns;
                }
                SqlNode node = getNthExpr( query, iAdjusted, sourceCount );
                String targetTypeString;
                String sourceTypeString;
                if ( PolyTypeUtil.areCharacterSetsMismatched( sourceType, targetType ) ) {
                    sourceTypeString = sourceType.getFullTypeString();
                    targetTypeString = targetType.getFullTypeString();
                } else {
                    sourceTypeString = sourceType.toString();
                    targetTypeString = targetType.toString();
                }
                throw newValidationError(
                        node,
                        RESOURCE.typeNotAssignable(
                                targetFields.get( i ).getName(),
                                targetTypeString,
                                sourceFields.get( i ).getName(),
                                sourceTypeString ) );
            }
            if ( targetType.getPolyType() == PolyType.JSON ) {
                String value = null;
                boolean needsCheck = false;
                if ( query instanceof SqlInsert && ((SqlInsert) query).getSource() instanceof SqlBasicCall source ) {
                    needsCheck = true;
                    if ( source.getOperator().getKind() == Kind.VALUES
                            && source.operands[0] instanceof SqlBasicCall
                            && ((SqlBasicCall) source.operands[0]).getOperator().getKind() == Kind.ROW ) {
                        SqlNode charNode = ((SqlBasicCall) source.operands[0]).operands[i];
                        if ( charNode instanceof SqlCharStringLiteral ) {
                            value = ((SqlCharStringLiteral) charNode).getValueAs( String.class );
                        }
                    } else if ( source.operands[i] instanceof SqlBasicCall ) {
                        SqlNode charNode = ((SqlBasicCall) source.operands[i]).operands[0];
                        if ( charNode instanceof SqlCharStringLiteral ) {
                            value = ((SqlCharStringLiteral) charNode).getValueAs( String.class );
                        }
                    }
                }

                // similar approach to PostgreSql the main difference for a JSON
                // field this validation compared to a normal text type
                if ( needsCheck && (value == null || !DocumentUtil.validateJson( value )) ) {
                    throw newValidationError(
                            query,
                            RESOURCE.notValidJson( value, "false" ) );
                }
            }
        }
    }


    /**
     * Locates the n'th expression in an INSERT or UPDATE query.
     *
     * @param query Query
     * @param ordinal Ordinal of expression
     * @param sourceCount Number of expressions
     * @return Ordinal'th expression, never null
     */
    private SqlNode getNthExpr( SqlNode query, int ordinal, int sourceCount ) {
        if ( query instanceof SqlInsert insert ) {
            if ( insert.getTargetColumnList() != null ) {
                return (SqlNode) insert.getTargetColumnList().get( ordinal );
            } else {
                return getNthExpr( insert.getSource(), ordinal, sourceCount );
            }
        } else if ( query instanceof SqlUpdate update ) {
            if ( update.getTargetColumnList() != null ) {
                return (SqlNode) update.getTargetColumnList().get( ordinal );
            } else if ( update.getSourceExpressionList() != null ) {
                return (SqlNode) update.getSourceExpressionList().get( ordinal );
            } else {
                return getNthExpr( update.getSourceSelect(), ordinal, sourceCount );
            }
        } else if ( query instanceof SqlSelect select ) {
            if ( select.getSqlSelectList().size() == sourceCount ) {
                return (SqlNode) select.getSqlSelectList().get( ordinal );
            } else {
                return query; // give up
            }
        } else {
            return query; // give up
        }
    }


    @Override
    public void validateDelete( SqlDelete call ) {
        final SqlSelect sqlSelect = call.getSourceSelect();
        validateSelect( sqlSelect, unknownType );

        final SqlValidatorNamespace targetNamespace = getSqlNamespace( call );
        validateNamespace( targetNamespace, unknownType );
        final Entity table = targetNamespace.getEntity();

        validateAccess( call.getTargetTable(), table, AccessEnum.DELETE );
    }


    @Override
    public void validateUpdate( SqlUpdate call ) {
        final SqlValidatorNamespace targetNamespace = getSqlNamespace( call );
        validateNamespace( targetNamespace, unknownType );
        final Entity table =
                SqlValidatorUtil.getLogicalEntity(
                        targetNamespace,
                        snapshot,
                        null,
                        null );

        final AlgDataType targetRowType =
                createTargetRowType(
                        table,
                        call.getTargetColumnList(),
                        true );

        final SqlSelect select = call.getSourceSelect();
        validateSelect( select, targetRowType );

        final AlgDataType sourceRowType = getSqlNamespace( call ).getTupleType();
        checkTypeAssignment( sourceRowType, targetRowType, call );

        validateAccess( call.getTargetTable(), table, AccessEnum.UPDATE );
    }


    @Override
    public void validateMerge( SqlMerge call ) {
        SqlSelect sqlSelect = call.getSourceSelect();
        // REVIEW zfong: Does an actual type have to be passed into validateSelect()?

        // REVIEW jvs:  In general, passing unknownType like this means we won't be able to correctly infer the types for dynamic parameter markers (SET x = ?).
        // But maybe validateUpdate and validateInsert below will do the job?

        // REVIEW ksecretan: They didn't get a chance to since validateSelect() would bail. Let's use the update/insert targetRowType when available.
        IdentifierNamespace targetNamespace = (IdentifierNamespace) getSqlNamespace( call.getTargetTable() );
        validateNamespace( targetNamespace, unknownType );

        Entity table = targetNamespace.getEntity();
        validateAccess( call.getTargetTable(), table, AccessEnum.UPDATE );

        AlgDataType targetRowType = unknownType;

        if ( call.getUpdateCall() != null ) {
            targetRowType =
                    createTargetRowType(
                            table,
                            call.getUpdateCall().getTargetColumnList(),
                            true );
        }
        if ( call.getInsertCall() != null ) {
            targetRowType =
                    createTargetRowType(
                            table,
                            call.getInsertCall().getTargetColumnList(),
                            false );
        }

        validateSelect( sqlSelect, targetRowType );

        if ( call.getUpdateCall() != null ) {
            validateUpdate( call.getUpdateCall() );
        }
        if ( call.getInsertCall() != null ) {
            validateInsert( call.getInsertCall() );
        }
    }


    /**
     * Validates access to a table.
     *
     * @param table Table
     * @param requiredAccess Access requested on table
     */
    private void validateAccess( SqlNode node, Entity table, AccessEnum requiredAccess ) {
        if ( table != null ) {
            AccessType access = AccessType.ALL;
            if ( !access.allowsAccess( requiredAccess ) ) {
                throw newValidationError( node, RESOURCE.accessNotAllowed( requiredAccess.name(), table.name ) );
            }
        }
    }


    /**
     * Validates a VALUES clause.
     *
     * @param node Values clause
     * @param targetRowType Row type which expression must conform to
     * @param scope Scope within which clause occurs
     */
    protected void validateValues( SqlCall node, AlgDataType targetRowType, final SqlValidatorScope scope ) {
        assert node.getKind() == Kind.VALUES;

        final List<Node> operands = node.getOperandList();
        for ( Node operand : operands ) {
            if ( !(operand.getKind() == Kind.ROW) ) {
                throw Util.needToImplement( "Values function where operands are scalars" );
            }

            SqlCall rowConstructor = (SqlCall) operand;
            if ( conformance.isInsertSubsetColumnsAllowed() && targetRowType.isStruct() && rowConstructor.operandCount() < targetRowType.getFieldCount() ) {
                targetRowType = typeFactory.createStructType( targetRowType.getFields().subList( 0, rowConstructor.operandCount() ) );
            } else if ( targetRowType.isStruct() && rowConstructor.operandCount() != targetRowType.getFieldCount() ) {
                return;
            }

            inferUnknownTypes( targetRowType, scope, rowConstructor );

            if ( targetRowType.isStruct() ) {
                for ( Pair<Node, AlgDataTypeField> pair : Pair.zip( rowConstructor.getOperandList(), targetRowType.getFields() ) ) {
                    if ( !pair.right.getType().isNullable() && CoreUtil.isNullLiteral( pair.left, false ) ) {
                        throw newValidationError( node, RESOURCE.columnNotNullable( pair.right.getName() ) );
                    }
                }
            }
        }

        for ( Node operand : operands ) {
            ((SqlNode) operand).validate( this, scope );
        }

        // validate that all row types have the same number of columns and that expressions in each column are compatible.
        // A values expression is turned into something that looks like ROW(type00, type01,...), ROW(type11,...),...
        final int rowCount = operands.size();
        if ( rowCount >= 2 ) {
            SqlCall firstRow = (SqlCall) operands.get( 0 );
            final int columnCount = firstRow.operandCount();

            // 1. check that all rows have the same cols length
            for ( Node operand : operands ) {
                SqlCall thisRow = (SqlCall) operand;
                if ( columnCount != thisRow.operandCount() ) {
                    throw newValidationError( node, RESOURCE.incompatibleValueType( OperatorRegistry.get( OperatorName.VALUES ).getName() ) );
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for ( int col = 0; col < columnCount; col++ ) {
                final int c = col;
                final AlgDataType type =
                        typeFactory.leastRestrictive(
                                new AbstractList<AlgDataType>() {
                                    @Override
                                    public AlgDataType get( int row ) {
                                        SqlCall thisRow = (SqlCall) operands.get( row );
                                        return deriveType( scope, thisRow.operand( c ) );
                                    }


                                    @Override
                                    public int size() {
                                        return rowCount;
                                    }
                                } );

                if ( null == type ) {
                    throw newValidationError( node, RESOURCE.incompatibleValueType( OperatorRegistry.get( OperatorName.VALUES ).getName() ) );
                }
            }
        }
    }


    @Override
    public void validateDataType( SqlDataTypeSpec dataType ) {
    }


    @Override
    public void validateDynamicParam( SqlDynamicParam dynamicParam ) {
    }


    /**
     * Throws a validator exception with access to the validator context.
     * The exception is determined when the function is applied.
     */
    public class ValidationErrorFunction implements Function2<SqlNode, Resources.ExInst<ValidatorException>, PolyphenyDbContextException> {

        @Override
        public PolyphenyDbContextException apply( SqlNode v0, Resources.ExInst<ValidatorException> v1 ) {
            return newValidationError( v0, v1 );
        }

    }


    @Override
    public PolyphenyDbContextException newValidationError( Node node, ExInst<ValidatorException> e ) {
        assert node != null;
        final ParserPos pos = node.getPos();
        return CoreUtil.newContextException( pos, e );
    }


    protected SqlWindow getWindowByName( SqlIdentifier id, SqlValidatorScope scope ) {
        SqlWindow window = null;
        if ( id.isSimple() ) {
            final String name = id.getSimple();
            window = scope.lookupWindow( name );
        }
        if ( window == null ) {
            throw newValidationError( id, RESOURCE.windowNotFound( id.toString() ) );
        }
        return window;
    }


    @Override
    public SqlWindow resolveWindow( SqlNode windowOrRef, SqlValidatorScope scope, boolean populateBounds ) {
        SqlWindow window;
        if ( windowOrRef instanceof SqlIdentifier ) {
            window = getWindowByName( (SqlIdentifier) windowOrRef, scope );
        } else {
            window = (SqlWindow) windowOrRef;
        }
        while ( true ) {
            final SqlIdentifier refId = window.getRefName();
            if ( refId == null ) {
                break;
            }
            final String refName = refId.getSimple();
            SqlWindow refWindow = scope.lookupWindow( refName );
            if ( refWindow == null ) {
                throw newValidationError( refId, RESOURCE.windowNotFound( refName ) );
            }
            window = window.overlay( refWindow, this );
        }

        if ( populateBounds ) {
            window.populateBounds();
        }
        return window;
    }


    public SqlNode getOriginal( SqlNode expr ) {
        SqlNode original = originalExprs.get( expr );
        if ( original == null ) {
            original = expr;
        }
        return original;
    }


    public void setOriginal( SqlNode expr, SqlNode original ) {
        // Don't overwrite the original.
        originalExprs.putIfAbsent( expr, original );
    }


    SqlValidatorNamespace lookupFieldNamespace( AlgDataType rowType, String name ) {
        final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
        final AlgDataTypeField field = nameMatcher.field( rowType, name );
        if ( field == null ) {
            return null;
        }
        return new FieldNamespace( this, field.getType() );
    }


    @Override
    public void validateWindow( SqlNode windowOrId, SqlValidatorScope scope, SqlCall call ) {
        // Enable nested aggregates with window aggregates (OVER operator)
        inWindow = true;

        final SqlWindow targetWindow = switch ( windowOrId.getKind() ) {
            case IDENTIFIER ->
                // Just verify the window exists in this query.  It will validate when the definition is processed
                    getWindowByName( (SqlIdentifier) windowOrId, scope );
            case WINDOW -> (SqlWindow) windowOrId;
            default -> throw Util.unexpected( windowOrId.getKind() );
        };

        assert targetWindow.getWindowCall() == null;
        targetWindow.setWindowCall( call );
        targetWindow.validate( this, scope );
        targetWindow.setWindowCall( null );
        call.validate( this, scope );

        validateAggregateParams( call, null, null, scope );

        // Disable nested aggregates post validation
        inWindow = false;
    }


    @Override
    public void validateMatchRecognize( SqlCall call ) {
        final SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
        final MatchRecognizeScope scope = (MatchRecognizeScope) getMatchRecognizeScope( matchRecognize );

        final MatchRecognizeNamespace ns = getSqlNamespace( call ).unwrap( MatchRecognizeNamespace.class );
        assert ns.rowType == null;

        // rows per match
        final Enum<?> rowsPerMatch = matchRecognize.getRowsPerMatch().value.asSymbol().value;
        final boolean allRows = rowsPerMatch == SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS;

        final AlgDataTypeFactory.Builder typeBuilder = typeFactory.builder();

        // parse PARTITION BY column
        SqlNodeList partitionBy = matchRecognize.getPartitionList();
        if ( partitionBy != null ) {
            for ( Node node : partitionBy ) {
                SqlIdentifier identifier = (SqlIdentifier) node;
                identifier.validate( this, scope );
                AlgDataType type = deriveType( scope, identifier );
                String name = identifier.names.get( 1 );
                typeBuilder.add( null, name, null, type );
            }
        }

        // parse ORDER BY column
        SqlNodeList orderBy = matchRecognize.getOrderList();
        if ( orderBy != null ) {
            for ( Node node : orderBy ) {
                ((SqlNode) node).validate( this, scope );
                SqlIdentifier identifier;
                if ( node instanceof SqlBasicCall ) {
                    identifier = (SqlIdentifier) ((SqlBasicCall) node).getOperands()[0];
                } else {
                    identifier = (SqlIdentifier) node;
                }

                if ( allRows ) {
                    AlgDataType type = deriveType( scope, identifier );
                    String name = identifier.names.get( 1 );
                    if ( !typeBuilder.nameExists( name ) ) {
                        typeBuilder.add( null, name, null, type );
                    }
                }
            }
        }

        if ( allRows ) {
            final SqlValidatorNamespace sqlNs = getSqlNamespace( matchRecognize.getTableRef() );
            final AlgDataType inputDataType = sqlNs.getTupleType();
            for ( AlgDataTypeField fs : inputDataType.getFields() ) {
                if ( !typeBuilder.nameExists( fs.getName() ) ) {
                    typeBuilder.add( fs );
                }
            }
        }

        // retrieve pattern variables used in pattern and subset
        SqlNode pattern = matchRecognize.getPattern();
        PatternVarVisitor visitor = new PatternVarVisitor( scope );
        pattern.accept( visitor );

        SqlLiteral interval = matchRecognize.getInterval();
        if ( interval != null ) {
            interval.validate( this, scope );
            if ( ((SqlIntervalLiteral) interval).signum() < 0 ) {
                throw newValidationError( interval, RESOURCE.intervalMustBeNonNegative( interval.toValue() ) );
            }
            if ( orderBy == null || orderBy.size() == 0 ) {
                throw newValidationError( interval, RESOURCE.cannotUseWithinWithoutOrderBy() );
            }

            SqlNode firstOrderByColumn = orderBy.getSqlList().get( 0 );
            SqlIdentifier identifier;
            if ( firstOrderByColumn instanceof SqlBasicCall ) {
                identifier = (SqlIdentifier) ((SqlBasicCall) firstOrderByColumn).getOperands()[0];
            } else {
                identifier = (SqlIdentifier) firstOrderByColumn;
            }
            AlgDataType firstOrderByColumnType = deriveType( scope, identifier );
            if ( firstOrderByColumnType.getPolyType() != PolyType.TIMESTAMP ) {
                throw newValidationError( interval, RESOURCE.firstColumnOfOrderByMustBeTimestamp() );
            }

            SqlNode expand = expand( interval, scope );
            AlgDataType type = deriveType( scope, expand );
            setValidatedNodeType( interval, type );
        }

        validateDefinitions( matchRecognize, scope );

        SqlNodeList subsets = matchRecognize.getSubsetList();
        if ( subsets != null && subsets.size() > 0 ) {
            for ( Node node : subsets ) {
                List<Node> operands = ((SqlCall) node).getOperandList();
                String leftString = ((SqlIdentifier) operands.get( 0 )).getSimple();
                if ( scope.getPatternVars().contains( leftString ) ) {
                    throw newValidationError( operands.get( 0 ), RESOURCE.patternVarAlreadyDefined( leftString ) );
                }
                scope.addPatternVar( leftString );
                for ( Node right : (SqlNodeList) operands.get( 1 ) ) {
                    SqlIdentifier id = (SqlIdentifier) right;
                    if ( !scope.getPatternVars().contains( id.getSimple() ) ) {
                        throw newValidationError( id, RESOURCE.unknownPattern( id.getSimple() ) );
                    }
                    scope.addPatternVar( id.getSimple() );
                }
            }
        }

        // validate AFTER ... SKIP TO
        final SqlNode skipTo = matchRecognize.getAfter();
        if ( skipTo instanceof SqlCall skipToCall ) {
            final SqlIdentifier id = skipToCall.operand( 0 );
            if ( !scope.getPatternVars().contains( id.getSimple() ) ) {
                throw newValidationError( id, RESOURCE.unknownPattern( id.getSimple() ) );
            }
        }

        List<Map.Entry<String, AlgDataType>> measureColumns = validateMeasure( matchRecognize, scope, allRows );
        for ( Map.Entry<String, AlgDataType> c : measureColumns ) {
            if ( !typeBuilder.nameExists( c.getKey() ) ) {
                typeBuilder.add( null, c.getKey(), null, c.getValue() );
            }
        }

        final AlgDataType rowType = typeBuilder.build();
        if ( matchRecognize.getMeasureList().size() == 0 ) {
            ns.setType( getSqlNamespace( matchRecognize.getTableRef() ).getTupleType() );
        } else {
            ns.setType( rowType );
        }
    }


    private List<Map.Entry<String, AlgDataType>> validateMeasure( SqlMatchRecognize mr, MatchRecognizeScope scope, boolean allRows ) {
        final List<String> aliases = new ArrayList<>();
        final List<SqlNode> sqlNodes = new ArrayList<>();
        final SqlNodeList measures = mr.getMeasureList();
        final List<Map.Entry<String, AlgDataType>> fields = new ArrayList<>();

        for ( Node measure : measures ) {
            assert measure instanceof SqlCall;
            final String alias = deriveAlias( (SqlNode) measure, aliases.size() );
            aliases.add( alias );

            SqlNode expand = expand( (SqlNode) measure, scope );
            expand = navigationInMeasure( expand, allRows );
            setOriginal( expand, (SqlNode) measure );

            inferUnknownTypes( unknownType, scope, expand );
            final AlgDataType type = deriveType( scope, expand );
            setValidatedNodeType( (SqlNode) measure, type );

            fields.add( Pair.of( alias, type ) );
            sqlNodes.add( (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall( ParserPos.ZERO, expand, new SqlIdentifier( alias, ParserPos.ZERO ) ) );
        }

        SqlNodeList list = new SqlNodeList( sqlNodes, measures.getPos() );
        inferUnknownTypes( unknownType, scope, list );

        for ( Node node : list ) {
            validateExpr( (SqlNode) node, scope );
        }

        mr.setOperand( SqlMatchRecognize.OPERAND_MEASURES, list );

        return fields;
    }


    private SqlNode navigationInMeasure( SqlNode node, boolean allRows ) {
        final Set<String> prefix = node.accept( new PatternValidator( true ) );
        Util.discard( prefix );
        final List<Node> ops = ((SqlCall) node).getOperandList();

        final Operator defaultOp =
                allRows
                        ? OperatorRegistry.get( OperatorName.RUNNING )
                        : OperatorRegistry.get( OperatorName.FINAL );
        final Node op0 = ops.get( 0 );
        if ( !isRunningOrFinal( op0.getKind() ) || !allRows && op0.getKind() == Kind.RUNNING ) {
            Node newNode = defaultOp.createCall( ParserPos.ZERO, op0 );
            node = (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall( ParserPos.ZERO, newNode, ops.get( 1 ) );
        }

        node = new NavigationExpander().go( node );
        return node;
    }


    private void validateDefinitions( SqlMatchRecognize mr, MatchRecognizeScope scope ) {
        final Set<String> aliases = NameMatchers.withCaseSensitive( false ).createSet();
        for ( SqlNode item : mr.getPatternDefList().getSqlList() ) {
            final String alias = alias( item );
            if ( !aliases.add( alias ) ) {
                throw newValidationError( item, Static.RESOURCE.patternVarAlreadyDefined( alias ) );
            }
            scope.addPatternVar( alias );
        }

        final List<SqlNode> sqlNodes = new ArrayList<>();
        for ( SqlNode item : mr.getPatternDefList().getSqlList() ) {
            final String alias = alias( item );
            SqlNode expand = expand( item, scope );
            expand = navigationInDefine( expand, alias );
            setOriginal( expand, item );

            inferUnknownTypes( booleanType, scope, expand );
            expand.validate( this, scope );

            // Some extra work need required here.
            // In PREV, NEXT, FINAL and LAST, only one pattern variable is allowed.
            sqlNodes.add(
                    (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall(
                            ParserPos.ZERO,
                            expand,
                            new SqlIdentifier( alias, ParserPos.ZERO ) ) );

            final AlgDataType type = deriveType( scope, expand );
            if ( !PolyTypeUtil.inBooleanFamily( type ) ) {
                throw newValidationError( expand, RESOURCE.condMustBeBoolean( "DEFINE" ) );
            }
            setValidatedNodeType( (SqlNode) item, type );
        }

        SqlNodeList list = new SqlNodeList( sqlNodes, mr.getPatternDefList().getPos() );
        inferUnknownTypes( unknownType, scope, list );
        for ( Node node : list ) {
            validateExpr( (SqlNode) node, scope );
        }
        mr.setOperand( SqlMatchRecognize.OPERAND_PATTERN_DEFINES, list );
    }


    /**
     * Returns the alias of a "expr AS alias" expression.
     */
    private static String alias( SqlNode item ) {
        assert item instanceof SqlCall;
        assert item.getKind() == Kind.AS;
        final SqlIdentifier identifier = ((SqlCall) item).operand( 1 );
        return identifier.getSimple();
    }


    /**
     * Checks that all pattern variables within a function are the same, and canonizes expressions such as {@code PREV(B.price)} to {@code LAST(B.price, 0)}.
     */
    private SqlNode navigationInDefine( SqlNode node, String alpha ) {
        Set<String> prefix = node.accept( new PatternValidator( false ) );
        Util.discard( prefix );
        node = new NavigationExpander().go( node );
        node = new NavigationReplacer( alpha ).go( node );
        return node;
    }


    @Override
    public void validateAggregateParams( SqlCall aggCall, SqlNode filter, SqlNodeList orderList, SqlValidatorScope scope ) {
        // For "agg(expr)", expr cannot itself contain aggregate function invocations.  For example, "SUM(2 * MAX(x))" is illegal; when we see it, we'll report the error for the SUM (not the MAX).
        // For more than one level of nesting, the error which results depends on the traversal order for validation.
        //
        // For a windowed aggregate "agg(expr)", expr can contain an aggregate function. For example,
        //   SELECT AVG(2 * MAX(x)) OVER (PARTITION BY y)
        //   FROM t
        //   GROUP BY y
        // is legal. Only one level of nesting is allowed since non-windowed aggregates cannot nest aggregates.

        // Store nesting level of each aggregate. If an aggregate is found at an invalid nesting level, throw an assert.
        final AggFinder a;
        if ( inWindow ) {
            a = overFinder;
        } else {
            a = aggOrOverFinder;
        }

        for ( SqlNode param : aggCall.getSqlOperandList() ) {
            if ( a.findAgg( param ) != null ) {
                throw newValidationError( aggCall, RESOURCE.nestedAggIllegal() );
            }
        }
        if ( filter != null ) {
            if ( a.findAgg( filter ) != null ) {
                throw newValidationError( filter, RESOURCE.aggregateInFilterIllegal() );
            }
        }
        if ( orderList != null ) {
            for ( SqlNode param : orderList.getSqlList() ) {
                if ( a.findAgg( param ) != null ) {
                    throw newValidationError( aggCall, RESOURCE.aggregateInWithinGroupIllegal() );
                }
            }
        }

        final SqlAggFunction op = (SqlAggFunction) aggCall.getOperator();
        switch ( op.requiresGroupOrder() ) {
            case MANDATORY:
                if ( orderList == null || orderList.size() == 0 ) {
                    throw newValidationError( aggCall, RESOURCE.aggregateMissingWithinGroupClause( op.getName() ) );
                }
                break;
            case OPTIONAL:
                break;
            case IGNORED:
                // rewrite the order list to empty
                if ( orderList != null ) {
                    orderList.getSqlList().clear();
                }
                break;
            case FORBIDDEN:
                if ( orderList != null && orderList.size() != 0 ) {
                    throw newValidationError( aggCall, RESOURCE.withinGroupClauseIllegalInAggregate( op.getName() ) );
                }
                break;
            default:
                throw new AssertionError( op );
        }
    }


    @Override
    public void validateCall( SqlCall call, SqlValidatorScope scope ) {
        final Operator operator = call.getOperator();
        if ( (call.operandCount() == 0)
                && (operator.getSyntax() == Syntax.FUNCTION_ID)
                && !call.isExpanded()
                && !conformance.allowNiladicParentheses() ) {
            // For example, "LOCALTIME()" is illegal. (It should be "LOCALTIME", which would have been handled as a SqlIdentifier.)
            throw handleUnresolvedFunction( call, (SqlFunction) operator, ImmutableList.of(), null );
        }

        SqlValidatorScope operandScope = scope.getOperandScope( call );

        if ( operator instanceof SqlFunction
                && ((SqlFunction) operator).getFunctionCategory()
                == FunctionCategory.MATCH_RECOGNIZE
                && !(operandScope instanceof MatchRecognizeScope) ) {
            throw newValidationError( call, Static.RESOURCE.functionMatchRecognizeOnly( call.toString() ) );
        }
        // Delegate validation to the operator.
        ((SqlOperator) operator).validateCall( call, this, scope, operandScope );
    }


    /**
     * Validates that a particular feature is enabled. By default, all features are enabled; subclasses may override this method to be more discriminating.
     *
     * @param feature feature being used, represented as a resource instance
     * @param context parser position context for error reporting, or null if
     */
    protected void validateFeature( Feature feature, ParserPos context ) {
        // By default, do nothing except to verify that the resource represents a real feature definition.
        assert feature.getProperties().get( "FeatureDefinition" ) != null;
    }


    @Override
    public SqlNode expand( SqlNode expr, SqlValidatorScope scope ) {
        final Expander expander = new Expander( this, scope );
        SqlNode newExpr = expr.accept( expander );
        if ( expr != newExpr ) {
            setOriginal( newExpr, expr );
        }
        return newExpr;
    }


    public SqlNode expandGroupByOrHavingExpr( SqlNode expr, SqlValidatorScope scope, SqlSelect select, boolean havingExpression ) {
        final Expander expander = new ExtendedExpander( this, scope, select, expr, havingExpression );
        SqlNode newExpr = expr.accept( expander );
        if ( expr != newExpr ) {
            setOriginal( newExpr, expr );
        }
        return newExpr;
    }


    @Override
    public boolean isSystemField( AlgDataTypeField field ) {
        return false;
    }


    @Override
    public List<List<String>> getFieldOrigins( Node sqlQuery ) {
        if ( sqlQuery instanceof SqlExplain ) {
            return Collections.emptyList();
        }
        final AlgDataType rowType = getValidatedNodeType( sqlQuery );
        final int fieldCount = rowType.getFieldCount();
        if ( !sqlQuery.isA( Kind.QUERY ) ) {
            return Collections.nCopies( fieldCount, null );
        }
        final List<List<String>> list = new ArrayList<>();
        for ( int i = 0; i < fieldCount; i++ ) {
            list.add( getFieldOrigin( (SqlNode) sqlQuery, i ) );
        }
        return ImmutableNullableList.copyOf( list );
    }


    private List<String> getFieldOrigin( SqlNode sqlQuery, int i ) {
        if ( sqlQuery instanceof SqlSelect sqlSelect ) {
            final SelectScope scope = getRawSelectScope( sqlSelect );
            final List<SqlNode> selectList = scope.getExpandedSelectList();
            final SqlNode selectItem = SqlUtil.stripAs( selectList.get( i ) );
            if ( selectItem instanceof SqlIdentifier ) {
                final SqlQualified qualified = scope.fullyQualify( (SqlIdentifier) selectItem );
                SqlValidatorNamespace namespace = qualified.namespace;
                final Entity table = namespace.getEntity();
                if ( table == null ) {
                    return null;
                }
                final List<String> origin = List.of( table.name );
                for ( String name : qualified.suffix() ) {
                    namespace = namespace.lookupChild( name );
                    if ( namespace == null ) {
                        return null;
                    }
                    origin.add( name );
                }
                return origin;
            }
            return null;
        } else if ( sqlQuery instanceof SqlOrderBy ) {
            return getFieldOrigin( ((SqlOrderBy) sqlQuery).query, i );
        } else {
            return null;
        }
    }


    @Override
    public AlgDataType getParameterRowType( Node sqlQuery ) {
        // NOTE: We assume that bind variables occur in depth-first tree traversal in the same order that they occurred in the SQL text.
        final List<AlgDataType> types = new ArrayList<>();
        // NOTE: but parameters on fetch/offset would be counted twice as they are counted in the SqlOrderBy call and the inner SqlSelect call
        final Set<SqlNode> alreadyVisited = new HashSet<>();
        sqlQuery.accept(
                new SqlShuttle() {

                    @Override
                    public SqlNode visit( DynamicParam param ) {
                        if ( alreadyVisited.add( (SqlNode) param ) ) {
                            AlgDataType type = getValidatedNodeType( param );
                            types.add( type );
                        }
                        return (SqlNode) param;
                    }
                } );
        return typeFactory.createStructType(
                types.stream().map( t -> (Long) null ).collect( Collectors.toList() ),
                types,
                IntStream.range( 0, types.size() ).mapToObj( i -> "?" + i ).collect( Collectors.toList() ) );
    }


    @Override
    public void validateColumnListParams( SqlFunction function, List<AlgDataType> argTypes, List<Node> operands ) {
        throw new UnsupportedOperationException();
    }


    private static boolean isPhysicalNavigation( Kind kind ) {
        return kind == Kind.PREV || kind == Kind.NEXT;
    }


    private static boolean isLogicalNavigation( Kind kind ) {
        return kind == Kind.FIRST || kind == Kind.LAST;
    }


    private static boolean isAggregation( Kind kind ) {
        return kind == Kind.SUM
                || kind == Kind.SUM0
                || kind == Kind.AVG
                || kind == Kind.COUNT
                || kind == Kind.MAX
                || kind == Kind.MIN;
    }


    private static boolean isRunningOrFinal( Kind kind ) {
        return kind == Kind.RUNNING || kind == Kind.FINAL;
    }


    private static boolean isSingleVarRequired( Kind kind ) {
        return isPhysicalNavigation( kind )
                || isLogicalNavigation( kind )
                || isAggregation( kind );
    }


    /**
     * Common base class for DML statement namespaces.
     */
    public static class DmlNamespace extends IdentifierNamespace {

        protected DmlNamespace( SqlValidatorImpl validator, SqlNode id, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
            super( validator, id, enclosingNode, parentScope );
        }

    }


    /**
     * Namespace for an INSERT statement.
     */
    private static class InsertNamespace extends DmlNamespace {

        private final SqlInsert node;


        InsertNamespace( SqlValidatorImpl validator, SqlInsert node, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
            super( validator, node.getTargetTable(), enclosingNode, parentScope );
            this.node = Objects.requireNonNull( node );
        }


        @Override
        public SqlInsert getNode() {
            return node;
        }

    }


    /**
     * Namespace for an UPDATE statement.
     */
    private static class UpdateNamespace extends DmlNamespace {

        private final SqlUpdate node;


        UpdateNamespace( SqlValidatorImpl validator, SqlUpdate node, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
            super( validator, node.getTargetTable(), enclosingNode, parentScope );
            this.node = Objects.requireNonNull( node );
        }


        @Override
        public SqlUpdate getNode() {
            return node;
        }

    }


    /**
     * Namespace for a DELETE statement.
     */
    private static class DeleteNamespace extends DmlNamespace {

        private final SqlDelete node;


        DeleteNamespace( SqlValidatorImpl validator, SqlDelete node, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
            super( validator, node.getTargetTable(), enclosingNode, parentScope );
            this.node = Objects.requireNonNull( node );
        }


        @Override
        public SqlDelete getNode() {
            return node;
        }

    }


    /**
     * Namespace for a MERGE statement.
     */
    private static class MergeNamespace extends DmlNamespace {

        private final SqlMerge node;


        MergeNamespace( SqlValidatorImpl validator, SqlMerge node, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
            super( validator, node.getTargetTable(), enclosingNode, parentScope );
            this.node = Objects.requireNonNull( node );
        }


        @Override
        public SqlMerge getNode() {
            return node;
        }

    }


    /**
     * retrieve pattern variables defined
     */
    private static class PatternVarVisitor implements NodeVisitor<Void> {

        private MatchRecognizeScope scope;


        PatternVarVisitor( MatchRecognizeScope scope ) {
            this.scope = scope;
        }


        @Override
        public Void visit( Literal literal ) {
            return null;
        }


        @Override
        public Void visit( Call call ) {
            for ( int i = 0; i < call.getOperandList().size(); i++ ) {
                call.getOperandList().get( i ).accept( this );
            }
            return null;
        }


        @Override
        public Void visit( NodeList nodeList ) {
            throw Util.needToImplement( nodeList );
        }


        @Override
        public Void visit( Identifier id ) {
            Preconditions.checkArgument( id.isSimple() );
            scope.addPatternVar( id.getSimple() );
            return null;
        }


        @Override
        public Void visit( DataTypeSpec type ) {
            throw Util.needToImplement( type );
        }


        @Override
        public Void visit( DynamicParam param ) {
            throw Util.needToImplement( param );
        }


        @Override
        public Void visit( IntervalQualifier intervalQualifier ) {
            throw Util.needToImplement( intervalQualifier );
        }

    }


    /**
     * Visitor which derives the type of a given {@link SqlNode}.
     * <p>
     * Each method must return the derived type. This visitor is basically a single-use dispatcher; the visit is never recursive.
     */
    private class DeriveTypeVisitor implements NodeVisitor<AlgDataType> {

        private final SqlValidatorScope scope;


        DeriveTypeVisitor( SqlValidatorScope scope ) {
            this.scope = scope;
        }


        @Override
        public AlgDataType visit( Literal literal ) {
            return ((SqlLiteral) literal).createSqlType( typeFactory );
        }


        @Override
        public AlgDataType visit( Call call ) {
            final Operator operator = call.getOperator();

            if ( operator instanceof SqlCrossMapItemOperator ) {
                return typeFactory.createPolyType( PolyType.VARCHAR, 255 );
            }

            return operator.deriveType( SqlValidatorImpl.this, scope, call );
        }


        @Override
        public AlgDataType visit( NodeList nodeList ) {
            // Operand is of a type that we can't derive a type for. If the operand is of a peculiar type, such as a SqlNodeList, then you should override the operator's validateCall() method so that it
            // doesn't try to validate that operand as an expression.
            throw Util.needToImplement( nodeList );
        }


        @Override
        public AlgDataType visit( Identifier id ) {
            // First check for builtin functions which don't have parentheses, like "LOCALTIME".
            SqlCall call = SqlUtil.makeCall( opTab, (SqlIdentifier) id );
            if ( call != null ) {
                return ((SqlOperator) call.getOperator()).validateOperands( SqlValidatorImpl.this, scope, call );
            }

            AlgDataType type = null;
            if ( !(scope instanceof EmptyScope) ) {
                id = scope.fullyQualify( (SqlIdentifier) id ).identifier;
            }

            // Resolve the longest prefix of id that we can
            int i;
            for ( i = id.getNames().size() - 1; i > 0; i-- ) {
                // REVIEW jvs: The name resolution rules used here are supposed to match SQL:2003 Part 2 Section 6.6 (identifier chain), but we don't currently have enough
                // information to get everything right.  In particular, routine parameters are currently looked up via resolve; we could do a better job if they were looked up via resolveColumn.

                final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
                final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
                scope.resolve( id.getNames().subList( 0, i ), false, resolved );
                if ( resolved.count() == 1 ) {
                    // There's a namespace with the name we seek.
                    final SqlValidatorScope.Resolve resolve = resolved.only();
                    type = resolve.rowType();
                    break;
                }
            }

            // Give precedence to namespace found, unless there are no more identifier components.
            if ( type == null || id.getNames().size() == 1 ) {
                // See if there's a column with the name we seek in precisely one of the namespaces in this scope.
                AlgDataType colType = scope.resolveColumn( id.getNames().get( 0 ), (SqlNode) id );
                if ( colType != null ) {
                    type = colType;
                }
                ++i;
            }

            if ( type == null ) {
                final SqlIdentifier last = ((SqlIdentifier) id).getComponent( i - 1, i );
                throw newValidationError( last, RESOURCE.unknownIdentifier( last.toString() ) );

            }

            // Resolve rest of identifier
            for ( ; i < id.getNames().size(); i++ ) {
                String name = id.getNames().get( i );
                final AlgDataTypeField field;
                if ( name.isEmpty() ) {
                    // The wildcard "*" is represented as an empty name. It never resolves to a field.
                    name = "*";
                    field = null;
                } else {
                    final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
                    field = nameMatcher.field( type, name );
                }
                if ( field == null ) {
                    throw newValidationError( ((SqlIdentifier) id).getComponent( i ), RESOURCE.unknownField( name ) );
                }
                type = field.getType();
            }
            type = PolyTypeUtil.addCharsetAndCollation( type, getTypeFactory() );
            return type;
        }


        @Override
        public AlgDataType visit( DataTypeSpec dataType ) {
            // Q. How can a data type have a type?
            // A. When it appears in an expression. (Say as the 2nd arg to the CAST operator.)
            validateDataType( (SqlDataTypeSpec) dataType );
            return ((SqlDataTypeSpec) dataType).deriveType( SqlValidatorImpl.this );
        }


        @Override
        public AlgDataType visit( DynamicParam param ) {
            return unknownType;
        }


        @Override
        public AlgDataType visit( IntervalQualifier intervalQualifier ) {
            return typeFactory.createIntervalType( intervalQualifier );
        }

    }


    /**
     * Converts an expression into canonical form by fully-qualifying any identifiers.
     */
    private static class Expander extends SqlScopedShuttle {

        protected final SqlValidatorImpl validator;


        Expander( SqlValidatorImpl validator, SqlValidatorScope scope ) {
            super( scope );
            this.validator = validator;
        }


        @Override
        public SqlNode visit( Identifier id ) {
            // First check for builtin functions which don't have parentheses, like "LOCALTIME".
            SqlCall call = SqlUtil.makeCall( validator.getOperatorTable(), (SqlIdentifier) id );
            if ( call != null ) {
                return call.accept( this );
            }
            final SqlIdentifier fqId = getScope().fullyQualify( (SqlIdentifier) id ).identifier;
            SqlNode expandedExpr = expandDynamicStar( (SqlIdentifier) id, fqId );
            validator.setOriginal( expandedExpr, (SqlNode) id );
            return expandedExpr;
        }


        @Override
        protected SqlNode visitScoped( SqlCall call ) {
            switch ( call.getKind() ) {
                case SCALAR_QUERY:
                case CURRENT_VALUE:
                case NEXT_VALUE:
                case WITH:
                    return call;
            }
            // Only visits arguments which are expressions. We don't want to qualify non-expressions such as 'x' in 'empno * 5 AS x'.
            ArgHandler<SqlNode> argHandler = new CallCopyingArgHandler( call, false );

            if ( SqlValidatorUtil.isNotRelational( validator ) && call.getKind() == Kind.OTHER_FUNCTION && call.getOperator().getOperatorName() == OperatorName.ITEM ) {
                return new SqlBasicCall( new SqlCrossMapItemOperator(), call.getOperandList().toArray( SqlNode[]::new ), ParserPos.ZERO );
            }

            call.getOperator().acceptCall( this, call, true, argHandler );
            final SqlNode result = argHandler.result();
            validator.setOriginal( result, call );
            return result;
        }


        protected SqlNode expandDynamicStar( SqlIdentifier id, SqlIdentifier fqId ) {
            if ( DynamicRecordType.isDynamicStarColName( Util.last( fqId.names ) ) && !DynamicRecordType.isDynamicStarColName( Util.last( id.names ) ) ) {
                // Convert a column ref into ITEM(*, 'col_name') for a dynamic star field in dynTable's rowType.
                SqlNode[] inputs = new SqlNode[2];
                inputs[0] = fqId;
                inputs[1] = SqlLiteral.createCharString( PolyString.of( Util.last( id.names ) ), id.getPos() );
                return new SqlBasicCall( OperatorRegistry.get( OperatorName.ITEM, SqlOperator.class ), inputs, id.getPos() );
            }
            return fqId;
        }

    }


    /**
     * Shuttle which walks over an expression in the ORDER BY clause, replacing usages of aliases with the underlying expression.
     */
    class OrderExpressionExpander extends SqlScopedShuttle {

        private final List<String> aliasList;
        private final SqlSelect select;
        private final SqlNode root;


        OrderExpressionExpander( SqlSelect select, SqlNode root ) {
            super( getOrderScope( select ) );
            this.select = select;
            this.root = root;
            this.aliasList = getSqlNamespace( select ).getTupleType().getFieldNames();
        }


        public SqlNode go() {
            return root.accept( this );
        }


        @Override
        public SqlNode visit( Literal literal ) {
            // Ordinal markers, e.g. 'select a, b from t order by 2'.
            // Only recognize them if they are the whole expression, and if the dialect permits.
            if ( literal == root && getConformance().isSortByOrdinal() ) {
                switch ( literal.getTypeName() ) {
                    case DECIMAL:
                    case DOUBLE:
                        final int intValue = literal.intValue( false );
                        if ( intValue >= 0 ) {
                            if ( intValue < 1 || intValue > aliasList.size() ) {
                                throw newValidationError( (Node) literal, RESOURCE.orderByOrdinalOutOfRange() );
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            int ordinal = intValue - 1;
                            return nthSelectItem( ordinal, literal.getPos() );
                        }
                        break;
                }
            }

            return super.visit( literal );
        }


        /**
         * Returns the <code>ordinal</code>th item in the select list.
         */
        private SqlNode nthSelectItem( int ordinal, final ParserPos pos ) {
            // TODO: Don't expand the list every time. Maybe keep an expanded version of each expression -- select lists and identifiers -- in the validator.

            SqlNodeList expandedSelectList =
                    expandStar(
                            select.getSqlSelectList(),
                            select,
                            false );
            Node expr = expandedSelectList.get( ordinal );
            expr = SqlUtil.stripAs( (SqlNode) expr );
            if ( expr instanceof SqlIdentifier ) {
                expr = getScope().fullyQualify( (SqlIdentifier) expr ).identifier;
            }

            // Create a copy of the expression with the position of the order item.
            return (SqlNode) expr.clone( pos );
        }


        @Override
        public SqlNode visit( Identifier id ) {
            // Aliases, e.g. 'select a as x, b from t order by x'.
            if ( id.isSimple() && getConformance().isSortByAlias() ) {
                String alias = id.getSimple();
                final SqlValidatorNamespace selectNs = getSqlNamespace( select );
                final AlgDataType rowType = selectNs.getRowTypeSansSystemColumns();
                final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
                AlgDataTypeField field = nameMatcher.field( rowType, alias );
                if ( field != null ) {
                    return nthSelectItem( field.getIndex(), id.getPos() );
                }
            }

            // No match. Return identifier unchanged.
            return getScope().fullyQualify( (SqlIdentifier) id ).identifier;
        }


        @Override
        protected SqlNode visitScoped( SqlCall call ) {
            // Don't attempt to expand sub-queries. We haven't implemented these yet.
            if ( call instanceof SqlSelect ) {
                return call;
            }
            return super.visitScoped( call );
        }

    }


    /**
     * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing usages of aliases or ordinals with the underlying expression.
     */
    static class ExtendedExpander extends Expander {

        final SqlSelect select;
        final SqlNode root;
        final boolean havingExpr;


        ExtendedExpander( SqlValidatorImpl validator, SqlValidatorScope scope, SqlSelect select, SqlNode root, boolean havingExpr ) {
            super( validator, scope );
            this.select = select;
            this.root = root;
            this.havingExpr = havingExpr;
        }


        @Override
        public SqlNode visit( Identifier id ) {
            if ( id.isSimple() && (havingExpr
                    ? validator.getConformance().isHavingAlias()
                    : validator.getConformance().isGroupByAlias()) ) {
                String name = id.getSimple();
                SqlNode expr = null;
                final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
                int n = 0;
                for ( Node s : select.getSqlSelectList() ) {
                    final String alias = SqlValidatorUtil.getAlias( (SqlNode) s, -1 );
                    if ( alias != null && nameMatcher.matches( alias, name ) ) {
                        expr = (SqlNode) s;
                        n++;
                    }
                }
                if ( n == 0 ) {
                    return super.visit( id );
                } else if ( n > 1 ) {
                    // More than one column has this alias.
                    throw validator.newValidationError( id, RESOURCE.columnAmbiguous( name ) );
                }
                if ( havingExpr && validator.isAggregate( root ) ) {
                    return super.visit( id );
                }
                expr = SqlUtil.stripAs( expr );
                if ( expr instanceof SqlIdentifier sid ) {
                    final SqlIdentifier fqId = getScope().fullyQualify( sid ).identifier;
                    expr = expandDynamicStar( sid, fqId );
                }
                return expr;
            }
            return super.visit( id );
        }


        @Override
        public SqlNode visit( Literal literal ) {
            if ( havingExpr || !validator.getConformance().isGroupByOrdinal() ) {
                return super.visit( literal );
            }
            boolean isOrdinalLiteral = literal == root;
            switch ( root.getKind() ) {
                case GROUPING_SETS:
                case ROLLUP:
                case CUBE:
                    if ( root instanceof SqlBasicCall ) {
                        List<Node> operandList = ((SqlBasicCall) root).getOperandList();
                        for ( Node node : operandList ) {
                            if ( node.equals( literal ) ) {
                                isOrdinalLiteral = true;
                                break;
                            }
                        }
                    }
                    break;
            }
            if ( isOrdinalLiteral ) {
                switch ( literal.getTypeName() ) {
                    case DECIMAL:
                    case DOUBLE:
                        final int intValue = literal.intValue( false );
                        if ( intValue >= 0 ) {
                            if ( intValue < 1 || intValue > select.getSqlSelectList().size() ) {
                                throw validator.newValidationError( literal, RESOURCE.orderByOrdinalOutOfRange() );
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            int ordinal = intValue - 1;
                            return SqlUtil.stripAs( (SqlNode) select.getSqlSelectList().get( ordinal ) );
                        }
                        break;
                }
            }

            return super.visit( literal );
        }

    }


    /**
     * Information about an identifier in a particular scope.
     */
    protected static class IdInfo {

        public final SqlValidatorScope scope;
        public final SqlIdentifier id;


        public IdInfo( SqlValidatorScope scope, SqlIdentifier id ) {
            this.scope = scope;
            this.id = id;
        }

    }


    /**
     * Utility object used to maintain information about the parameters in a function call.
     */
    protected static class FunctionParamInfo {

        /**
         * Maps a cursor (based on its position relative to other cursor parameters within a function call) to the SELECT associated with the cursor.
         */
        public final Map<Integer, SqlSelect> cursorPosToSelectMap;

        /**
         * Maps a column list parameter to the parent cursor parameter it references. The parameters are id'd by their names.
         */
        public final Map<String, String> columnListParamToParentCursorMap;


        public FunctionParamInfo() {
            cursorPosToSelectMap = new HashMap<>();
            columnListParamToParentCursorMap = new HashMap<>();
        }

    }


    /**
     * Modify the nodes in navigation function such as FIRST, LAST, PREV AND NEXT.
     */
    private static class NavigationModifier extends SqlShuttle {

        public SqlNode go( SqlNode node ) {
            return node.accept( this );
        }

    }


    /**
     * Shuttle that expands navigation expressions in a MATCH_RECOGNIZE clause.
     * <p>
     * Examples:
     *
     * <ul>
     * <li>{@code PREV(A.price + A.amount)} &rarr; {@code PREV(A.price) + PREV(A.amount)}</li>
     * <li>{@code FIRST(A.price * 2)} &rarr; {@code FIRST(A.PRICE) * 2}</li>
     * </ul>
     */
    private static class NavigationExpander extends NavigationModifier {

        final SqlOperator op;
        final SqlNode offset;


        NavigationExpander() {
            this( null, null );
        }


        NavigationExpander( SqlOperator operator, SqlNode offset ) {
            this.offset = offset;
            this.op = operator;
        }


        @Override
        public SqlNode visit( Call call ) {
            Kind kind = call.getKind();
            List<Node> operands = call.getOperandList();
            List<SqlNode> newOperands = new ArrayList<>();
            if ( isLogicalNavigation( kind ) || isPhysicalNavigation( kind ) ) {
                Node inner = operands.get( 0 );
                Node offset = operands.get( 1 );

                // merge two straight prev/next, update offset
                if ( isPhysicalNavigation( kind ) ) {
                    Kind innerKind = inner.getKind();
                    if ( isPhysicalNavigation( innerKind ) ) {
                        List<Node> innerOperands = ((SqlCall) inner).getOperandList();
                        Node innerOffset = innerOperands.get( 1 );
                        Operator newOperator =
                                innerKind == kind
                                        ? OperatorRegistry.get( OperatorName.PLUS )
                                        : OperatorRegistry.get( OperatorName.MINUS );
                        offset = newOperator.createCall( ParserPos.ZERO, offset, innerOffset );
                        inner = call.getOperator().createCall( ParserPos.ZERO, innerOperands.get( 0 ), offset );
                    }
                }
                SqlNode newInnerNode = inner.accept( new NavigationExpander( (SqlOperator) call.getOperator(), (SqlNode) offset ) );
                if ( op != null ) {
                    newInnerNode = (SqlNode) op.createCall( ParserPos.ZERO, newInnerNode, this.offset );
                }
                return newInnerNode;
            }

            if ( operands.size() > 0 ) {
                for ( Node node : operands ) {
                    if ( node != null ) {
                        Node newNode = node.accept( new NavigationExpander() );
                        if ( op != null ) {
                            newNode = op.createCall( ParserPos.ZERO, newNode, offset );
                        }
                        newOperands.add( (SqlNode) newNode );
                    } else {
                        newOperands.add( null );
                    }
                }
                return (SqlNode) call.getOperator().createCall( ParserPos.ZERO, newOperands );
            } else {
                if ( op == null ) {
                    return (SqlNode) call;
                } else {
                    return (SqlNode) op.createCall( ParserPos.ZERO, call, offset );
                }
            }
        }


        @Override
        public SqlNode visit( Identifier id ) {
            if ( op == null ) {
                return (SqlNode) id;
            } else {
                return (SqlNode) op.createCall( ParserPos.ZERO, id, offset );
            }
        }

    }


    /**
     * Shuttle that replaces {@code A as A.price > PREV(B.price)} with {@code PREV(A.price, 0) > LAST(B.price, 0)}.
     * <p>
     * Replacing {@code A.price} with {@code PREV(A.price, 0)} makes the implementation of {@link RexVisitor#visitPatternFieldRef(RexPatternFieldRef)} more unified.
     * Otherwise, it's difficult to implement this method. If it returns the specified field, then the navigation such as {@code PREV(A.price, 1)} becomes impossible; if not, then comparisons such as {@code A.price > PREV(A.price, 1)} become meaningless.
     */
    private static class NavigationReplacer extends NavigationModifier {

        private final String alpha;


        NavigationReplacer( String alpha ) {
            this.alpha = alpha;
        }


        @Override
        public SqlNode visit( Call call ) {
            Kind kind = call.getKind();
            if ( isLogicalNavigation( kind ) || isAggregation( kind ) || isRunningOrFinal( kind ) ) {
                return (SqlNode) call;
            }

            if ( Objects.requireNonNull( kind ) == Kind.PREV ) {
                final List<Node> operands = call.getOperandList();
                if ( operands.get( 0 ) instanceof SqlIdentifier ) {
                    String name = ((SqlIdentifier) operands.get( 0 )).names.get( 0 );
                    return (SqlNode) (name.equals( alpha )
                            ? call
                            : OperatorRegistry.get( OperatorName.LAST ).createCall( ParserPos.ZERO, operands ));
                }
            }
            return super.visit( call );
        }


        @Override
        public SqlNode visit( Identifier id ) {
            if ( id.isSimple() ) {
                return (SqlNode) id;
            }
            Operator operator = id.getNames().get( 0 ).equals( alpha )
                    ? OperatorRegistry.get( OperatorName.PREV )
                    : OperatorRegistry.get( OperatorName.LAST );

            return (SqlNode) operator.createCall(
                    ParserPos.ZERO,
                    id,
                    SqlLiteral.createExactNumeric( "0", ParserPos.ZERO ) );
        }

    }


    /**
     * Within one navigation function, the pattern var should be same
     */
    private class PatternValidator extends BasicNodeVisitor<Set<String>> {

        private final boolean isMeasure;
        int firstLastCount;
        int prevNextCount;
        int aggregateCount;


        PatternValidator( boolean isMeasure ) {
            this( isMeasure, 0, 0, 0 );
        }


        PatternValidator( boolean isMeasure, int firstLastCount, int prevNextCount, int aggregateCount ) {
            this.isMeasure = isMeasure;
            this.firstLastCount = firstLastCount;
            this.prevNextCount = prevNextCount;
            this.aggregateCount = aggregateCount;
        }


        @Override
        public Set<String> visit( Call call ) {
            boolean isSingle = false;
            Set<String> vars = new HashSet<>();
            Kind kind = call.getKind();
            List<Node> operands = call.getOperandList();

            if ( isSingleVarRequired( kind ) ) {
                isSingle = true;
                if ( isPhysicalNavigation( kind ) ) {
                    if ( isMeasure ) {
                        throw newValidationError( call, Static.RESOURCE.patternPrevFunctionInMeasure( call.toString() ) );
                    }
                    if ( firstLastCount != 0 ) {
                        throw newValidationError( call, Static.RESOURCE.patternPrevFunctionOrder( call.toString() ) );
                    }
                    prevNextCount++;
                } else if ( isLogicalNavigation( kind ) ) {
                    if ( firstLastCount != 0 ) {
                        throw newValidationError( call, Static.RESOURCE.patternPrevFunctionOrder( call.toString() ) );
                    }
                    firstLastCount++;
                } else if ( isAggregation( kind ) ) {
                    // cannot apply aggregation in PREV/NEXT, FIRST/LAST
                    if ( firstLastCount != 0 || prevNextCount != 0 ) {
                        throw newValidationError( call, Static.RESOURCE.patternAggregationInNavigation( call.toString() ) );
                    }
                    if ( kind == Kind.COUNT && call.getOperandList().size() > 1 ) {
                        throw newValidationError( call, Static.RESOURCE.patternCountFunctionArg() );
                    }
                    aggregateCount++;
                }
            }

            if ( isRunningOrFinal( kind ) && !isMeasure ) {
                throw newValidationError( call, Static.RESOURCE.patternRunningFunctionInDefine( call.toString() ) );
            }

            for ( Node node : operands ) {
                if ( node != null ) {
                    vars.addAll(
                            node.accept(
                                    new PatternValidator(
                                            isMeasure,
                                            firstLastCount,
                                            prevNextCount,
                                            aggregateCount ) ) );
                }
            }

            if ( isSingle ) {
                if ( Objects.requireNonNull( kind ) == Kind.COUNT ) {
                    if ( vars.size() > 1 ) {
                        throw newValidationError( call, RESOURCE.patternCountFunctionArg() );
                    }
                } else {
                    if ( operands.size() == 0
                            || !(operands.get( 0 ) instanceof SqlCall)
                            || !((SqlCall) operands.get( 0 )).getOperator().equals( OperatorRegistry.get( OperatorName.CLASSIFIER ) ) ) {
                        if ( vars.isEmpty() ) {
                            throw newValidationError( call, RESOURCE.patternFunctionNullCheck( call.toString() ) );
                        }
                        if ( vars.size() != 1 ) {
                            throw newValidationError( call, RESOURCE.patternFunctionVariableCheck( call.toString() ) );
                        }
                    }
                }
            }
            return vars;
        }


        @Override
        public Set<String> visit( Identifier identifier ) {
            boolean check = prevNextCount > 0 || firstLastCount > 0 || aggregateCount > 0;
            Set<String> vars = new HashSet<>();
            if ( identifier.getNames().size() > 1 && check ) {
                vars.add( identifier.getNames().get( 0 ) );
            }
            return vars;
        }


        @Override
        public Set<String> visit( Literal literal ) {
            return ImmutableSet.of();
        }


        @Override
        public Set<String> visit( IntervalQualifier qualifier ) {
            return ImmutableSet.of();
        }


        @Override
        public Set<String> visit( DataTypeSpec type ) {
            return ImmutableSet.of();
        }


        @Override
        public Set<String> visit( DynamicParam param ) {
            return ImmutableSet.of();
        }

    }


    /**
     * Permutation of fields in NATURAL JOIN or USING.
     */
    private class Permute {

        final List<ImmutableList<Integer>> sources;
        final AlgDataType rowType;
        final boolean trivial;


        Permute( SqlNode from, int offset ) {
            if ( Objects.requireNonNull( from.getKind() ) == Kind.JOIN ) {
                final SqlJoin join = (SqlJoin) from;
                final Permute left = new Permute( join.getLeft(), offset );
                final int fieldCount = getValidatedNodeType( join.getLeft() ).getFields().size();
                final Permute right = new Permute( join.getRight(), offset + fieldCount );
                final List<String> names = usingNames( join );
                final List<ImmutableList<Integer>> sources = new ArrayList<>();
                final Set<ImmutableList<Integer>> sourceSet = new HashSet<>();
                final Builder b = typeFactory.builder();
                if ( names != null ) {
                    for ( String name : names ) {
                        final AlgDataTypeField f = left.field( name );
                        final ImmutableList<Integer> source = left.sources.get( f.getIndex() );
                        sourceSet.add( source );
                        final AlgDataTypeField f2 = right.field( name );
                        final ImmutableList<Integer> source2 = right.sources.get( f2.getIndex() );
                        sourceSet.add( source2 );
                        sources.add( ImmutableList.copyOf( Stream.concat( source.stream(), source2.stream() ).collect( Collectors.toList() ) ) );
                        final boolean nullable =
                                (f.getType().isNullable()
                                        || join.getJoinType().generatesNullsOnLeft())
                                        && (f2.getType().isNullable()
                                        || join.getJoinType().generatesNullsOnRight());
                        b.add( f ).nullable( nullable );
                    }
                }
                for ( AlgDataTypeField f : left.rowType.getFields() ) {
                    final ImmutableList<Integer> source = left.sources.get( f.getIndex() );
                    if ( sourceSet.add( source ) ) {
                        sources.add( source );
                        b.add( f );
                    }
                }
                for ( AlgDataTypeField f : right.rowType.getFields() ) {
                    final ImmutableList<Integer> source = right.sources.get( f.getIndex() );
                    if ( sourceSet.add( source ) ) {
                        sources.add( source );
                        b.add( f );
                    }
                }
                rowType = b.build();
                this.sources = ImmutableList.copyOf( sources );
                this.trivial = left.trivial && right.trivial && (names == null || names.isEmpty());
            } else {
                rowType = getValidatedNodeType( from );
                this.sources = Functions.generate( rowType.getFieldCount(), i -> ImmutableList.of( offset + i ) );
                this.trivial = true;
            }
        }


        private AlgDataTypeField field( String name ) {
            return NameMatchers.withCaseSensitive( false ).field( rowType, name );
        }


        /**
         * Returns the set of field names in the join condition specified by USING or implicitly by NATURAL, de-duplicated and in order.
         */
        private List<String> usingNames( SqlJoin join ) {
            switch ( join.getConditionType() ) {
                case USING:
                    final ImmutableList.Builder<String> list = ImmutableList.builder();
                    final Set<String> names = NameMatchers.withCaseSensitive( false ).createSet();
                    for ( Node node : (SqlNodeList) join.getCondition() ) {
                        final String name = ((SqlIdentifier) node).getSimple();
                        if ( names.add( name ) ) {
                            list.add( name );
                        }
                    }
                    return list.build();
                case NONE:
                    if ( join.isNatural() ) {
                        final AlgDataType t0 = getValidatedNodeType( join.getLeft() );
                        final AlgDataType t1 = getValidatedNodeType( join.getRight() );
                        return SqlValidatorUtil.deriveNaturalJoinColumnList( NameMatchers.withCaseSensitive( false ), t0, t1 );
                    }
            }
            return null;
        }


        /**
         * Moves fields according to the permutation.
         */
        public void permute( List<SqlNode> selectItems, List<AlgDataTypeField> fields ) {
            if ( trivial ) {
                return;
            }

            final List<SqlNode> oldSelectItems = ImmutableList.copyOf( selectItems );
            selectItems.clear();
            final List<AlgDataTypeField> oldFields = ImmutableList.copyOf( fields );
            fields.clear();
            for ( ImmutableList<Integer> source : sources ) {
                final int p0 = source.get( 0 );
                AlgDataTypeField field = oldFields.get( p0 );
                final String name = field.getName();
                AlgDataType type = field.getType();
                SqlNode selectItem = oldSelectItems.get( p0 );
                for ( int p1 : Util.skip( source ) ) {
                    final AlgDataTypeField field1 = oldFields.get( p1 );
                    final SqlNode selectItem1 = oldSelectItems.get( p1 );
                    final AlgDataType type1 = field1.getType();
                    // output is nullable only if both inputs are
                    final boolean nullable = type.isNullable() && type1.isNullable();
                    final AlgDataType type2 = PolyTypeUtil.leastRestrictiveForComparison( typeFactory, type, type1 );
                    selectItem =
                            (SqlNode) OperatorRegistry.get( OperatorName.AS ).createCall(
                                    ParserPos.ZERO,
                                    OperatorRegistry.get( OperatorName.COALESCE ).createCall(
                                            ParserPos.ZERO,
                                            maybeCast( selectItem, type, type2 ),
                                            maybeCast( selectItem1, type1, type2 ) ),
                                    new SqlIdentifier( name, ParserPos.ZERO ) );
                    type = typeFactory.createTypeWithNullability( type2, nullable );
                }
                fields.add( new AlgDataTypeFieldImpl( 1L, name, fields.size(), type ) );
                selectItems.add( selectItem );
            }
        }

    }


    /**
     * Validation status.
     */
    public enum Status {
        /**
         * Validation has not started for this scope.
         */
        UNVALIDATED,

        /**
         * Validation is in progress for this scope.
         */
        IN_PROGRESS,

        /**
         * Validation has completed (perhaps unsuccessfully).
         */
        VALID
    }

}
