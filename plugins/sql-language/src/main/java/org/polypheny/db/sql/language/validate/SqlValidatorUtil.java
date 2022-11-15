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

package org.polypheny.db.sql.language.validate;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.ValidatorCatalogReader;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.plan.AlgOptSchemaWithSampling;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.schema.CustomColumnResolvingTable;
import org.polypheny.db.schema.ExtensibleTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Utility methods related to validation.
 */
public class SqlValidatorUtil {

    private SqlValidatorUtil() {
    }


    /**
     * Converts a {@link SqlValidatorScope} into a {@link AlgOptTable}. This is only possible if the scope represents an identifier, such as "sales.emp".
     * Otherwise, returns null.
     *
     * @param namespace Namespace
     * @param catalogReader Schema
     * @param datasetName Name of sample dataset to substitute, or null to use the regular table
     * @param usedDataset Output parameter which is set to true if a sample dataset is found; may be null
     */
    public static AlgOptTable getAlgOptTable( SqlValidatorNamespace namespace, Prepare.CatalogReader catalogReader, String datasetName, boolean[] usedDataset ) {
        if ( namespace.isWrapperFor( TableNamespace.class ) ) {
            final TableNamespace tableNamespace = namespace.unwrap( TableNamespace.class );
            return getAlgOptTable( tableNamespace, catalogReader, datasetName, usedDataset, tableNamespace.extendedFields );
        } else if ( namespace.isWrapperFor( SqlValidatorImpl.DmlNamespace.class ) ) {
            final SqlValidatorImpl.DmlNamespace dmlNamespace = namespace.unwrap( SqlValidatorImpl.DmlNamespace.class );
            final SqlValidatorNamespace resolvedNamespace = dmlNamespace.resolve();
            if ( resolvedNamespace.isWrapperFor( TableNamespace.class ) ) {
                final TableNamespace tableNamespace = resolvedNamespace.unwrap( TableNamespace.class );
                final ValidatorTable validatorTable = tableNamespace.getTable();
                final AlgDataTypeFactory typeFactory = catalogReader.getTypeFactory();
                final List<AlgDataTypeField> extendedFields =
                        dmlNamespace.extendList == null
                                ? ImmutableList.of()
                                : getExtendedColumns( typeFactory, validatorTable, dmlNamespace.extendList );
                return getAlgOptTable( tableNamespace, catalogReader, datasetName, usedDataset, extendedFields );
            }
        }
        return null;
    }


    private static AlgOptTable getAlgOptTable( TableNamespace tableNamespace, Prepare.CatalogReader catalogReader, String datasetName, boolean[] usedDataset, List<AlgDataTypeField> extendedFields ) {
        final List<String> names = tableNamespace.getTable().getQualifiedName();
        AlgOptTable table;
        if ( datasetName != null && catalogReader instanceof AlgOptSchemaWithSampling ) {
            final AlgOptSchemaWithSampling reader = (AlgOptSchemaWithSampling) catalogReader;
            table = reader.getTableForMember( names, datasetName, usedDataset );
        } else {
            // Schema does not support substitution. Ignore the data set, if any.
            table = catalogReader.getTableForMember( names );
        }
        if ( !extendedFields.isEmpty() ) {
            table = table.extend( extendedFields );
        }
        return table;
    }


    /**
     * Gets a list of extended columns with field indices to the underlying table.
     */
    public static List<AlgDataTypeField> getExtendedColumns( AlgDataTypeFactory typeFactory, ValidatorTable table, SqlNodeList extendedColumns ) {
        final ImmutableList.Builder<AlgDataTypeField> extendedFields = ImmutableList.builder();
        final ExtensibleTable extTable = table.unwrap( ExtensibleTable.class );
        int extendedFieldOffset =
                extTable == null
                        ? table.getRowType().getFieldCount()
                        : extTable.getExtendedColumnOffset();
        for ( final Pair<SqlIdentifier, SqlDataTypeSpec> pair : pairs( extendedColumns ) ) {
            final SqlIdentifier identifier = pair.left;
            final SqlDataTypeSpec type = pair.right;
            extendedFields.add( new AlgDataTypeFieldImpl( identifier.toString(), extendedFieldOffset++, type.deriveType( typeFactory ) ) );
        }
        return extendedFields.build();
    }


    /**
     * Converts a list of extended columns (of the form [name0, type0, name1, type1, ...] into a list of (name, type) pairs.
     */
    private static List<Pair<SqlIdentifier, SqlDataTypeSpec>> pairs( SqlNodeList extendedColumns ) {
        final List list = extendedColumns.getList();
        //noinspection unchecked
        return Util.pairs( list );
    }


    /**
     * Gets a map of indexes from the source to fields in the target for the intersecting set of source and target fields.
     *
     * @param sourceFields The source of column names that determine indexes
     * @param targetFields The target fields to be indexed
     */
    public static ImmutableMap<Integer, AlgDataTypeField> getIndexToFieldMap( List<AlgDataTypeField> sourceFields, AlgDataType targetFields ) {
        final ImmutableMap.Builder<Integer, AlgDataTypeField> output = ImmutableMap.builder();
        for ( final AlgDataTypeField source : sourceFields ) {
            final AlgDataTypeField target = targetFields.getField( source.getName(), true, false );
            if ( target != null ) {
                output.put( source.getIndex(), target );
            }
        }
        return output.build();
    }


    /**
     * Gets the bit-set to the column ordinals in the source for columns that intersect in the target.
     *
     * @param sourceRowType The source upon which to ordinate the bit set.
     * @param targetRowType The target to overlay on the source to create the bit set.
     */
    public static ImmutableBitSet getOrdinalBitSet( AlgDataType sourceRowType, AlgDataType targetRowType ) {
        Map<Integer, AlgDataTypeField> indexToField = getIndexToFieldMap( sourceRowType.getFieldList(), targetRowType );
        return getOrdinalBitSet( sourceRowType, indexToField );
    }


    /**
     * Gets the bit-set to the column ordinals in the source for columns that intersect in the target.
     *
     * @param sourceRowType The source upon which to ordinate the bit set.
     * @param indexToField The map of ordinals to target fields.
     */
    public static ImmutableBitSet getOrdinalBitSet( AlgDataType sourceRowType, Map<Integer, AlgDataTypeField> indexToField ) {
        ImmutableBitSet source = ImmutableBitSet.of( Lists.transform( sourceRowType.getFieldList(), AlgDataTypeField::getIndex ) );
        ImmutableBitSet target = ImmutableBitSet.of( indexToField.keySet() );
        return source.intersect( target );
    }


    /**
     * Checks that there are no duplicates in a list of {@link SqlIdentifier}.
     */
    public static void checkIdentifierListForDuplicates( List<SqlNode> columnList, SqlValidatorImpl.ValidationErrorFunction validationErrorFunction ) {
        final List<List<String>> names = columnList.stream().map( o -> ((SqlIdentifier) o).names ).collect( Collectors.toList() );
        final int i = Util.firstDuplicate( names );
        if ( i >= 0 ) {
            throw validationErrorFunction.apply( columnList.get( i ), Static.RESOURCE.duplicateNameInColumnList( Util.last( names.get( i ) ) ) );
        }
    }


    /**
     * Converts an expression "expr" into "expr AS alias".
     */
    public static Node addAlias( SqlNode expr, String alias ) {
        final ParserPos pos = expr.getPos();
        final SqlIdentifier id = new SqlIdentifier( alias, pos );
        return OperatorRegistry.get( OperatorName.AS ).createCall( pos, expr, id );
    }


    /**
     * Derives an alias for a node, and invents a mangled identifier if it cannot.
     *
     * Examples:
     *
     * <ul>
     * <li>Alias: "1 + 2 as foo" yields "foo"</li>
     * <li>Identifier: "foo.bar.baz" yields "baz"</li>
     * <li>Anything else yields "expr$<i>ordinal</i>"</li>
     * </ul>
     *
     * @return An alias, if one can be derived; or a synthetic alias "expr$<i>ordinal</i>" if ordinal &lt; 0; otherwise null
     */
    public static String getAlias( SqlNode node, int ordinal ) {
        switch ( node.getKind() ) {
            case AS:
                // E.g. "1 + 2 as foo" --> "foo"
                return ((SqlCall) node).operand( 1 ).toString();

            case OVER:
                // E.g. "bids over w" --> "bids"
                return getAlias( ((SqlCall) node).operand( 0 ), ordinal );

            case IDENTIFIER:
                // E.g. "foo.bar" --> "bar"
                return Util.last( ((SqlIdentifier) node).names );

            default:
                if ( ordinal < 0 ) {
                    return null;
                } else {
                    return CoreUtil.deriveAliasFromOrdinal( ordinal );
                }
        }
    }


    /**
     * Factory method for {@link SqlValidator}.
     */
    public static SqlValidatorWithHints newValidator( OperatorTable opTab, ValidatorCatalogReader catalogReader, AlgDataTypeFactory typeFactory, Conformance conformance ) {
        return new SqlValidatorImpl( opTab, catalogReader, typeFactory, conformance );
    }


    public static AlgDataTypeField getTargetField( AlgDataType rowType, AlgDataTypeFactory typeFactory, SqlIdentifier id, ValidatorCatalogReader catalogReader, AlgOptTable table ) {
        return getTargetField( rowType, typeFactory, id, catalogReader, table, false );
    }


    /**
     * Resolve a target column name in the target table.
     *
     * @param rowType the target row type
     * @param id the target column identifier
     * @param table the target table or null if it is not a RelOptTable instance
     * @return the target field or null if the name cannot be resolved
     */
    public static AlgDataTypeField getTargetField( AlgDataType rowType, AlgDataTypeFactory typeFactory, SqlIdentifier id, ValidatorCatalogReader catalogReader, AlgOptTable table, boolean isDocument ) {
        final Table t = table == null ? null : table.unwrap( Table.class );

        if ( !(t instanceof CustomColumnResolvingTable) ) {
            final NameMatcher nameMatcher = catalogReader.nameMatcher();
            AlgDataTypeField typeField = nameMatcher.field( rowType, id.getSimple() );

            if ( typeField == null && isDocument ) {
                return new AlgDataTypeFieldImpl( id.getSimple(), -1, new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.JSON, 300 ) );
            }

            return typeField;
        }

        final List<Pair<AlgDataTypeField, List<String>>> entries = ((CustomColumnResolvingTable) t).resolveColumn( rowType, typeFactory, id.names );
        switch ( entries.size() ) {
            case 1:
                if ( !entries.get( 0 ).getValue().isEmpty() ) {
                    return null;
                }
                return entries.get( 0 ).getKey();
            default:
                return null;
        }
    }


    /**
     * Resolves a multi-part identifier such as "SCHEMA.EMP.EMPNO" to a namespace. The returned namespace, never null, may represent a schema, table, column, etc.
     */
    public static SqlValidatorNamespace lookup( SqlValidatorScope scope, List<String> names ) {
        assert names.size() > 0;
        final NameMatcher nameMatcher = scope.getValidator().getCatalogReader().nameMatcher();
        final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
        scope.resolve( ImmutableList.of( names.get( 0 ) ), nameMatcher, false, resolved );
        assert resolved.count() == 1;
        SqlValidatorNamespace namespace = resolved.only().namespace;
        for ( String name : Util.skip( names ) ) {
            namespace = namespace.lookupChild( name );
            assert namespace != null;
        }
        return namespace;
    }


    public static void getSchemaObjectMonikers( ValidatorCatalogReader catalogReader, List<String> names, List<Moniker> hints ) {
        // Assume that the last name is 'dummy' or similar.
        List<String> subNames = Util.skipLast( names );

        // Try successively with catalog.schema, catalog and no prefix
        for ( List<String> x : catalogReader.getSchemaPaths() ) {
            final List<String> names2 = ImmutableList.<String>builder().addAll( x ).addAll( subNames ).build();
            hints.addAll( catalogReader.getAllSchemaObjectNames( names2 ) );
        }
    }


    public static SelectScope getEnclosingSelectScope( SqlValidatorScope scope ) {
        while ( scope instanceof DelegatingScope ) {
            if ( scope instanceof SelectScope ) {
                return (SelectScope) scope;
            }
            scope = ((DelegatingScope) scope).getParent();
        }
        return null;
    }


    public static AggregatingSelectScope getEnclosingAggregateSelectScope( SqlValidatorScope scope ) {
        while ( scope instanceof DelegatingScope ) {
            if ( scope instanceof AggregatingSelectScope ) {
                return (AggregatingSelectScope) scope;
            }
            scope = ((DelegatingScope) scope).getParent();
        }
        return null;
    }


    /**
     * Derives the list of column names suitable for NATURAL JOIN. These are the columns that occur exactly once on each side of the join.
     *
     * @param nameMatcher Whether matches are case-sensitive
     * @param leftRowType Row type of left input to the join
     * @param rightRowType Row type of right input to the join
     * @return List of columns that occur once on each side
     */
    public static List<String> deriveNaturalJoinColumnList( NameMatcher nameMatcher, AlgDataType leftRowType, AlgDataType rightRowType ) {
        final List<String> naturalColumnNames = new ArrayList<>();
        final List<String> leftNames = leftRowType.getFieldNames();
        final List<String> rightNames = rightRowType.getFieldNames();
        for ( String name : leftNames ) {
            if ( nameMatcher.frequency( leftNames, name ) == 1 && nameMatcher.frequency( rightNames, name ) == 1 ) {
                naturalColumnNames.add( name );
            }
        }
        return naturalColumnNames;
    }


    /**
     * Analyzes an expression in a GROUP BY clause.
     *
     * It may be an expression, an empty list (), or a call to {@code GROUPING SETS}, {@code CUBE}, {@code ROLLUP}, {@code TUMBLE}, {@code HOP}
     * or {@code SESSION}.
     *
     * Each group item produces a list of group sets, which are written to {@code topBuilder}. To find the grouping sets of the query, we will take
     * the cartesian product of the group sets.
     */
    public static void analyzeGroupItem( SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, ImmutableList.Builder<ImmutableList<ImmutableBitSet>> topBuilder, SqlNode groupExpr ) {
        final ImmutableList.Builder<ImmutableBitSet> builder;
        switch ( groupExpr.getKind() ) {
            case CUBE:
            case ROLLUP:
                // E.g. ROLLUP(a, (b, c)) becomes [{0}, {1, 2}] then we roll up to [(0, 1, 2), (0), ()]  -- note no (0, 1)
                List<ImmutableBitSet> bitSets = analyzeGroupTuple( scope, groupAnalyzer, ((SqlCall) groupExpr).getOperandList() );
                switch ( groupExpr.getKind() ) {
                    case ROLLUP:
                        topBuilder.add( rollup( bitSets ) );
                        return;
                    default:
                        topBuilder.add( cube( bitSets ) );
                        return;
                }
            case OTHER:
                if ( groupExpr instanceof SqlNodeList ) {
                    SqlNodeList list = (SqlNodeList) groupExpr;
                    for ( Node node : list ) {
                        analyzeGroupItem( scope, groupAnalyzer, topBuilder, (SqlNode) node );
                    }
                    return;
                }
                // fall through
            case HOP:
            case TUMBLE:
            case SESSION:
            case GROUPING_SETS:
            default:
                builder = ImmutableList.builder();
                convertGroupSet( scope, groupAnalyzer, builder, groupExpr );
                topBuilder.add( builder.build() );
        }
    }


    /**
     * Analyzes a GROUPING SETS item in a GROUP BY clause.
     */
    private static void convertGroupSet( SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, ImmutableList.Builder<ImmutableBitSet> builder, SqlNode groupExpr ) {
        switch ( groupExpr.getKind() ) {
            case GROUPING_SETS:
                final SqlCall call = (SqlCall) groupExpr;
                for ( Node node : call.getOperandList() ) {
                    convertGroupSet( scope, groupAnalyzer, builder, (SqlNode) node );
                }
                return;
            case ROW:
                final List<ImmutableBitSet> bitSets = analyzeGroupTuple( scope, groupAnalyzer, ((SqlCall) groupExpr).getOperandList() );
                builder.add( ImmutableBitSet.union( bitSets ) );
                return;
            case ROLLUP:
            case CUBE: {
                // GROUPING SETS ( (a), ROLLUP(c,b), CUBE(d,e) )
                // is EQUIVALENT to
                // GROUPING SETS ( (a), (c,b), (b) ,(), (d,e), (d), (e) ).
                // Expand all ROLLUP/CUBE nodes
                List<ImmutableBitSet> operandBitSet = analyzeGroupTuple( scope, groupAnalyzer, ((SqlCall) groupExpr).getOperandList() );
                switch ( groupExpr.getKind() ) {
                    case ROLLUP:
                        builder.addAll( rollup( operandBitSet ) );
                        return;
                    default:
                        builder.addAll( cube( operandBitSet ) );
                        return;
                }
            }
            default:
                builder.add( analyzeGroupExpr( scope, groupAnalyzer, groupExpr ) );
                return;
        }
    }


    /**
     * Analyzes a tuple in a GROUPING SETS clause.
     *
     * For example, in {@code GROUP BY GROUPING SETS ((a, b), a, c)}, {@code (a, b)} is a tuple.
     *
     * Gathers into {@code groupExprs} the set of distinct expressions being grouped, and returns a bitmap indicating which expressions this tuple
     * is grouping.
     */
    private static List<ImmutableBitSet> analyzeGroupTuple( SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, List<Node> operandList ) {
        List<ImmutableBitSet> list = new ArrayList<>();
        for ( Node operand : operandList ) {
            list.add( analyzeGroupExpr( scope, groupAnalyzer, (SqlNode) operand ) );
        }
        return list;
    }


    /**
     * Analyzes a component of a tuple in a GROUPING SETS clause.
     */
    private static ImmutableBitSet analyzeGroupExpr( SqlValidatorScope scope, GroupAnalyzer groupAnalyzer, SqlNode groupExpr ) {
        final SqlNode expandedGroupExpr = scope.getValidator().expand( groupExpr, scope );

        switch ( expandedGroupExpr.getKind() ) {
            case ROW:
                return ImmutableBitSet.union( analyzeGroupTuple( scope, groupAnalyzer, ((SqlCall) expandedGroupExpr).getOperandList() ) );
            case OTHER:
                if ( expandedGroupExpr instanceof SqlNodeList && ((SqlNodeList) expandedGroupExpr).size() == 0 ) {
                    return ImmutableBitSet.of();
                }
        }

        final int ref = lookupGroupExpr( groupAnalyzer, groupExpr );
        if ( expandedGroupExpr instanceof SqlIdentifier ) {
            // SQL 2003 does not allow expressions of column references
            SqlIdentifier expr = (SqlIdentifier) expandedGroupExpr;

            // column references should be fully qualified.
            assert expr.names.size() == 2;
            String originalRelName = expr.names.get( 0 );
            String originalFieldName = expr.names.get( 1 );

            final NameMatcher nameMatcher = scope.getValidator().getCatalogReader().nameMatcher();
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            scope.resolve( ImmutableList.of( originalRelName ), nameMatcher, false, resolved );

            assert resolved.count() == 1;
            final SqlValidatorScope.Resolve resolve = resolved.only();
            final AlgDataType rowType = resolve.rowType();
            final int childNamespaceIndex = resolve.path.steps().get( 0 ).i;

            int namespaceOffset = 0;

            if ( childNamespaceIndex > 0 ) {
                // If not the first child, need to figure out the width of output types from all the preceding namespaces
                final SqlValidatorScope ancestorScope = resolve.scope;
                assert ancestorScope instanceof ListScope;
                List<SqlValidatorNamespace> children = ((ListScope) ancestorScope).getChildren();

                for ( int j = 0; j < childNamespaceIndex; j++ ) {
                    namespaceOffset += children.get( j ).getRowType().getFieldCount();
                }
            }

            AlgDataTypeField field = nameMatcher.field( rowType, originalFieldName );
            int origPos = namespaceOffset + field.getIndex();

            groupAnalyzer.groupExprProjection.put( origPos, ref );
        }

        return ImmutableBitSet.of( ref );
    }


    private static int lookupGroupExpr( GroupAnalyzer groupAnalyzer, SqlNode expr ) {
        for ( Ord<SqlNode> node : Ord.zip( groupAnalyzer.groupExprs ) ) {
            if ( node.e.equalsDeep( expr, Litmus.IGNORE ) ) {
                return node.i;
            }
        }

        switch ( expr.getKind() ) {
            case HOP:
            case TUMBLE:
            case SESSION:
                groupAnalyzer.extraExprs.add( expr );
                break;
        }
        groupAnalyzer.groupExprs.add( expr );
        return groupAnalyzer.groupExprs.size() - 1;
    }


    /**
     * Computes the rollup of bit sets.
     *
     * For example, <code>rollup({0}, {1})</code> returns <code>({0, 1}, {0}, {})</code>.
     *
     * Bit sets are not necessarily singletons: <code>rollup({0, 2}, {3, 5})</code> returns <code>({0, 2, 3, 5}, {0, 2}, {})</code>.
     */
    @VisibleForTesting
    public static ImmutableList<ImmutableBitSet> rollup( List<ImmutableBitSet> bitSets ) {
        Set<ImmutableBitSet> builder = new LinkedHashSet<>();
        for ( ; ; ) {
            final ImmutableBitSet union = ImmutableBitSet.union( bitSets );
            builder.add( union );
            if ( union.isEmpty() ) {
                break;
            }
            bitSets = bitSets.subList( 0, bitSets.size() - 1 );
        }
        return ImmutableList.copyOf( builder );
    }


    /**
     * Computes the cube of bit sets.
     *
     * For example,  <code>rollup({0}, {1})</code> returns <code>({0, 1}, {0}, {})</code>.
     *
     * Bit sets are not necessarily singletons: <code>rollup({0, 2}, {3, 5})</code> returns <code>({0, 2, 3, 5}, {0, 2}, {})</code>.
     */
    @VisibleForTesting
    public static ImmutableList<ImmutableBitSet> cube( List<ImmutableBitSet> bitSets ) {
        // Given the bit sets [{1}, {2, 3}, {5}], form the lists [[{1}, {}], [{2, 3}, {}], [{5}, {}]].
        final Set<List<ImmutableBitSet>> builder = new LinkedHashSet<>();
        for ( ImmutableBitSet bitSet : bitSets ) {
            builder.add( Arrays.asList( bitSet, ImmutableBitSet.of() ) );
        }
        Set<ImmutableBitSet> flattenedBitSets = new LinkedHashSet<>();
        for ( List<ImmutableBitSet> o : Linq4j.product( builder ) ) {
            flattenedBitSets.add( ImmutableBitSet.union( o ) );
        }
        return ImmutableList.copyOf( flattenedBitSets );
    }


    /**
     * Returns whether there are any input columns that are sorted.
     *
     * If so, it can be the default ORDER BY clause for a WINDOW specification. (This is an extension to the SQL standard for streaming.)
     */
    public static boolean containsMonotonic( SqlValidatorScope scope ) {
        for ( SqlValidatorNamespace ns : children( scope ) ) {
            ns = ns.resolve();
            for ( String field : ns.getRowType().getFieldNames() ) {
                if ( !ns.getMonotonicity( field ).mayRepeat() ) {
                    return true;
                }
            }
        }
        return false;
    }


    private static List<SqlValidatorNamespace> children( SqlValidatorScope scope ) {
        return scope instanceof ListScope
                ? ((ListScope) scope).getChildren()
                : ImmutableList.of();
    }


    /**
     * Returns whether any of the given expressions are sorted.
     *
     * If so, it can be the default ORDER BY clause for a WINDOW specification.
     * (This is an extension to the SQL standard for streaming.)
     */
    static boolean containsMonotonic( SelectScope scope, SqlNodeList nodes ) {
        for ( Node node : nodes ) {
            if ( !scope.getMonotonicity( (SqlNode) node ).mayRepeat() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Builds a list of GROUP BY expressions.
     */
    static class GroupAnalyzer {

        /**
         * Extra expressions, computed from the input as extra GROUP BY expressions. For example, calls to the {@code TUMBLE} functions.
         */
        final List<SqlNode> extraExprs = new ArrayList<>();
        final List<SqlNode> groupExprs;
        final Map<Integer, Integer> groupExprProjection = new HashMap<>();
        int groupCount;


        GroupAnalyzer( List<SqlNode> groupExprs ) {
            this.groupExprs = groupExprs;
        }


        SqlNode createGroupExpr() {
            // TODO: create an expression that could have no other source
            return SqlLiteral.createCharString( "xyz" + groupCount++, ParserPos.ZERO );
        }

    }

}

