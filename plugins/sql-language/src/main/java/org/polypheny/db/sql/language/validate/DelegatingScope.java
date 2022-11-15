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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.constant.MonikerType;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DynamicRecordType;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.schema.CustomColumnResolvingTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.MonikerImpl;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * A scope which delegates all requests to its parent scope. Use this as a base class for defining nested scopes.
 */
public abstract class DelegatingScope implements SqlValidatorScope {

    /**
     * Parent scope. This is where to look next to resolve an identifier; it is not always the parent object in the parse tree.
     *
     * This is never null: at the top of the tree, it is an {@link EmptyScope}.
     */
    protected final SqlValidatorScope parent;
    protected final SqlValidatorImpl validator;


    /**
     * Creates a <code>DelegatingScope</code>.
     *
     * @param parent Parent scope
     */
    DelegatingScope( SqlValidatorScope parent ) {
        super();
        assert parent != null;
        this.validator = (SqlValidatorImpl) parent.getValidator();
        this.parent = parent;
    }


    @Override
    public void addChild( SqlValidatorNamespace ns, String alias, boolean nullable ) {
        // By default, you cannot add to a scope. Derived classes can override.
        throw new UnsupportedOperationException();
    }


    @Override
    public void resolve( List<String> names, NameMatcher nameMatcher, boolean deep, Resolved resolved ) {
        parent.resolve( names, nameMatcher, deep, resolved );
    }


    /**
     * If a record type allows implicit references to fields, recursively looks into the fields. Otherwise returns immediately.
     */
    void resolveInNamespace( SqlValidatorNamespace ns, boolean nullable, List<String> names, NameMatcher nameMatcher, Path path, Resolved resolved ) {
        if ( names.isEmpty() ) {
            resolved.found( ns, nullable, this, path, null );
            return;
        }
        final AlgDataType rowType = ns.getRowType();
        if ( rowType.isStruct() ) {
            ValidatorTable validatorTable = ns.getTable();
            if ( validatorTable instanceof Prepare.PreparingTable ) {
                Table t = ((Prepare.PreparingTable) validatorTable).unwrap( Table.class );
                if ( t instanceof CustomColumnResolvingTable ) {
                    final List<Pair<AlgDataTypeField, List<String>>> entries = ((CustomColumnResolvingTable) t).resolveColumn( rowType, validator.getTypeFactory(), names );
                    for ( Pair<AlgDataTypeField, List<String>> entry : entries ) {
                        final AlgDataTypeField field = entry.getKey();
                        final List<String> remainder = entry.getValue();
                        final SqlValidatorNamespace ns2 = new FieldNamespace( validator, field.getType() );
                        final Step path2 = path.plus( rowType, field.getIndex(), field.getName(), StructKind.FULLY_QUALIFIED );
                        resolveInNamespace( ns2, nullable, remainder, nameMatcher, path2, resolved );
                    }
                    return;
                }
            }

            final String name = names.get( 0 );
            final AlgDataTypeField field0 = nameMatcher.field( rowType, name );
            if ( field0 != null ) {
                final SqlValidatorNamespace ns2 = ns.lookupChild( field0.getName() );
                final Step path2 = path.plus( rowType, field0.getIndex(), field0.getName(), StructKind.FULLY_QUALIFIED );
                resolveInNamespace( ns2, nullable, names.subList( 1, names.size() ), nameMatcher, path2, resolved );
            } else {
                for ( AlgDataTypeField field : rowType.getFieldList() ) {
                    switch ( field.getType().getStructKind() ) {
                        case PEEK_FIELDS:
                        case PEEK_FIELDS_DEFAULT:
                        case PEEK_FIELDS_NO_EXPAND:
                            final Step path2 = path.plus( rowType, field.getIndex(), field.getName(), field.getType().getStructKind() );
                            final SqlValidatorNamespace ns2 = ns.lookupChild( field.getName() );
                            resolveInNamespace( ns2, nullable, names, nameMatcher, path2, resolved );
                    }
                }
            }
        }
    }


    protected void addColumnNames( SqlValidatorNamespace ns, List<Moniker> colNames ) {
        final AlgDataType rowType;
        try {
            rowType = ns.getRowType();
        } catch ( Error e ) {
            // namespace is not good - bail out.
            return;
        }

        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            colNames.add( new MonikerImpl( field.getName(), MonikerType.COLUMN ) );
        }
    }


    @Override
    public void findAllColumnNames( List<Moniker> result ) {
        parent.findAllColumnNames( result );
    }


    @Override
    public void findAliases( Collection<Moniker> result ) {
        parent.findAliases( result );
    }


    @Override
    @SuppressWarnings("deprecation")
    public Pair<String, SqlValidatorNamespace> findQualifyingTableName( String columnName, SqlNode ctx ) {
        //noinspection deprecation
        return parent.findQualifyingTableName( columnName, ctx );
    }


    @Override
    public Map<String, ScopeChild> findQualifyingTableNames( String columnName, SqlNode ctx, NameMatcher nameMatcher ) {
        return parent.findQualifyingTableNames( columnName, ctx, nameMatcher );
    }


    @Override
    public AlgDataType resolveColumn( String name, SqlNode ctx ) {
        return parent.resolveColumn( name, ctx );
    }


    @Override
    public AlgDataType nullifyType( SqlNode node, AlgDataType type ) {
        return parent.nullifyType( node, type );
    }


    @Override
    @SuppressWarnings("deprecation")
    public SqlValidatorNamespace getTableNamespace( List<String> names ) {
        return parent.getTableNamespace( names );
    }


    @Override
    public void resolveTable( List<String> names, NameMatcher nameMatcher, Path path, Resolved resolved ) {
        parent.resolveTable( names, nameMatcher, path, resolved );
    }


    @Override
    public SqlValidatorScope getOperandScope( SqlCall call ) {
        if ( call instanceof SqlSelect ) {
            return validator.getSelectScope( (SqlSelect) call );
        }
        return this;
    }


    @Override
    public SqlValidator getValidator() {
        return validator;
    }


    /**
     * Converts an identifier into a fully-qualified identifier. For example, the "empno" in "select empno from emp natural join dept" becomes "emp.empno".
     *
     * If the identifier cannot be resolved, throws. Never returns null.
     */
    @Override
    public SqlQualified fullyQualify( SqlIdentifier identifier ) {
        if ( identifier.isStar() ) {
            return SqlQualified.create( this, 1, null, identifier );
        }

        final SqlIdentifier previous = identifier;
        final NameMatcher nameMatcher = validator.catalogReader.nameMatcher();
        String columnName;
        final String tableName;
        final SqlValidatorNamespace namespace;
        switch ( identifier.names.size() ) {
            case 1: {
                columnName = identifier.names.get( 0 );
                final Map<String, ScopeChild> map = findQualifyingTableNames( columnName, identifier, nameMatcher );
                switch ( map.size() ) {
                    case 0:
                        if ( nameMatcher.isCaseSensitive() ) {
                            final NameMatcher liberalMatcher = NameMatchers.liberal();
                            final Map<String, ScopeChild> map2 = findQualifyingTableNames( columnName, identifier, liberalMatcher );
                            if ( !map2.isEmpty() ) {
                                final List<String> list = new ArrayList<>();
                                for ( ScopeChild entry : map2.values() ) {
                                    final AlgDataTypeField field = liberalMatcher.field( entry.namespace.getRowType(), columnName );
                                    list.add( field.getName() );
                                }
                                Collections.sort( list );
                                throw validator.newValidationError( identifier, Static.RESOURCE.columnNotFoundDidYouMean( columnName, Util.sepList( list, "', '" ) ) );
                            }
                        }
                        throw validator.newValidationError( identifier, Static.RESOURCE.columnNotFound( columnName ) );
                    case 1:
                        tableName = map.keySet().iterator().next();
                        namespace = map.get( tableName ).namespace;
                        break;
                    default:
                        throw validator.newValidationError( identifier, Static.RESOURCE.columnAmbiguous( columnName ) );
                }

                final ResolvedImpl resolved = new ResolvedImpl();
                resolveInNamespace( namespace, false, identifier.names, nameMatcher, Path.EMPTY, resolved );
                final AlgDataTypeField field = nameMatcher.field( namespace.getRowType(), columnName );
                if ( field != null ) {
                    if ( hasAmbiguousUnresolvedStar( namespace.getRowType(), field, columnName ) ) {
                        throw validator.newValidationError( identifier, Static.RESOURCE.columnAmbiguous( columnName ) );
                    }

                    columnName = field.getName(); // use resolved field name
                }
                // todo: do implicit collation here
                final ParserPos pos = identifier.getPos();
                identifier =
                        new SqlIdentifier(
                                ImmutableList.of( tableName, columnName ),
                                null,
                                pos,
                                ImmutableList.of( ParserPos.ZERO, pos ) );
            }
            // fall through
            default: {
                SqlValidatorNamespace fromNs = null;
                Path fromPath = null;
                AlgDataType fromRowType = null;
                final ResolvedImpl resolved = new ResolvedImpl();
                int size = identifier.names.size();
                int i = size - 1;
                for ( ; i > 0; i-- ) {
                    final SqlIdentifier prefix = identifier.getComponent( 0, i );
                    resolved.clear();
                    resolve( prefix.names, nameMatcher, false, resolved );
                    if ( resolved.count() == 1 ) {
                        final Resolve resolve = resolved.only();
                        fromNs = resolve.namespace;
                        fromPath = resolve.path;
                        fromRowType = resolve.rowType();
                        break;
                    }
                    // Look for a table alias that is the wrong case.
                    if ( nameMatcher.isCaseSensitive() ) {
                        final NameMatcher liberalMatcher = NameMatchers.liberal();
                        resolved.clear();
                        resolve( prefix.names, liberalMatcher, false, resolved );
                        if ( resolved.count() == 1 ) {
                            final Step lastStep = Util.last( resolved.only().path.steps() );
                            throw validator.newValidationError(
                                    prefix,
                                    Static.RESOURCE.tableNameNotFoundDidYouMean( prefix.toString(), lastStep.name ) );
                        }
                    }
                }
                if ( fromNs == null || fromNs instanceof SchemaNamespace ) {
                    // Look for a column not qualified by a table alias.
                    columnName = identifier.names.get( 0 );
                    final Map<String, ScopeChild> map = findQualifyingTableNames( columnName, identifier, nameMatcher );
                    switch ( map.size() ) {
                        default:
                            final SqlIdentifier prefix1 = identifier.skipLast( 1 );
                            throw validator.newValidationError( prefix1, Static.RESOURCE.tableNameNotFound( prefix1.toString() ) );
                        case 1: {
                            final Map.Entry<String, ScopeChild> entry = map.entrySet().iterator().next();
                            final String tableName2 = map.keySet().iterator().next();
                            fromNs = entry.getValue().namespace;
                            fromPath = Path.EMPTY;

                            // Adding table name is for RecordType column with StructKind.PEEK_FIELDS or StructKind.PEEK_FIELDS only.
                            // Access to a field in a RecordType column of other StructKind should always be qualified with table name.
                            final AlgDataTypeField field = nameMatcher.field( fromNs.getRowType(), columnName );
                            if ( field != null ) {
                                switch ( field.getType().getStructKind() ) {
                                    case PEEK_FIELDS:
                                    case PEEK_FIELDS_DEFAULT:
                                    case PEEK_FIELDS_NO_EXPAND:
                                        columnName = field.getName(); // use resolved field name
                                        resolve( ImmutableList.of( tableName2 ), nameMatcher, false, resolved );
                                        if ( resolved.count() == 1 ) {
                                            final Resolve resolve = resolved.only();
                                            fromNs = resolve.namespace;
                                            fromPath = resolve.path;
                                            fromRowType = resolve.rowType();
                                            identifier = identifier
                                                    .setName( 0, columnName )
                                                    .add( 0, tableName2, ParserPos.ZERO );
                                            ++i;
                                            ++size;
                                        }
                                        break;
                                    default:
                                        // Throw an error if the table was not found.
                                        // If one or more of the child namespaces allows peeking (e.g. if they are Phoenix column families) then we relax the SQL standard requirement that record fields are qualified by table alias.
                                        final SqlIdentifier prefix = identifier.skipLast( 1 );
                                        throw validator.newValidationError( prefix, Static.RESOURCE.tableNameNotFound( prefix.toString() ) );
                                }
                            }
                        }
                    }
                }

                // If a table alias is part of the identifier, make sure that the table alias uses the same case as it was defined. For example, in
                //
                //    SELECT e.empno FROM Emp as E
                //
                // change "e.empno" to "E.empno".
                if ( fromNs.getEnclosingNode() != null && !(this instanceof MatchRecognizeScope) ) {
                    String alias = SqlValidatorUtil.getAlias( fromNs.getEnclosingNode(), -1 );
                    if ( alias != null && i > 0 && !alias.equals( identifier.names.get( i - 1 ) ) ) {
                        identifier = identifier.setName( i - 1, alias );
                    }
                }
                if ( fromPath.stepCount() > 1 ) {
                    assert fromRowType != null;
                    for ( Step p : fromPath.steps() ) {
                        fromRowType = fromRowType.getFieldList().get( p.i ).getType();
                    }
                    ++i;
                }
                final SqlIdentifier suffix = identifier.getComponent( i, size );
                resolved.clear();
                resolveInNamespace( fromNs, false, suffix.names, nameMatcher, Path.EMPTY, resolved );
                final Path path;
                switch ( resolved.count() ) {
                    case 0:
                        // Maybe the last component was correct, just wrong case
                        if ( nameMatcher.isCaseSensitive() ) {
                            NameMatcher liberalMatcher = NameMatchers.liberal();
                            resolved.clear();
                            resolveInNamespace( fromNs, false, suffix.names, liberalMatcher, Path.EMPTY, resolved );
                            if ( resolved.count() > 0 ) {
                                int k = size - 1;
                                final SqlIdentifier prefix = identifier.getComponent( 0, i );
                                final SqlIdentifier suffix3 = identifier.getComponent( i, k + 1 );
                                final Step step = Util.last( resolved.resolves.get( 0 ).path.steps() );
                                throw validator.newValidationError( suffix3, Static.RESOURCE.columnNotFoundInTableDidYouMean( suffix3.toString(), prefix.toString(), step.name ) );
                            }
                        }
                        // Find the shortest suffix that also fails. Suppose we cannot resolve "a.b.c"; we find we cannot resolve "a.b" but can resolve "a". So, the error will be "Column 'a.b' not found".
                        int k = size - 1;
                        for ( ; k > i; --k ) {
                            SqlIdentifier suffix2 = identifier.getComponent( i, k );
                            resolved.clear();
                            resolveInNamespace( fromNs, false, suffix2.names, nameMatcher, Path.EMPTY, resolved );
                            if ( resolved.count() > 0 ) {
                                break;
                            }
                        }
                        final SqlIdentifier prefix = identifier.getComponent( 0, i );
                        final SqlIdentifier suffix3 = identifier.getComponent( i, k + 1 );
                        throw validator.newValidationError( suffix3, Static.RESOURCE.columnNotFoundInTable( suffix3.toString(), prefix.toString() ) );
                    case 1:
                        path = resolved.only().path;
                        break;
                    default:
                        final Comparator<Resolve> c =
                                new Comparator<Resolve>() {
                                    @Override
                                    public int compare( Resolve o1, Resolve o2 ) {
                                        // Name resolution that uses fewer implicit steps wins.
                                        int c = Integer.compare( worstKind( o1.path ), worstKind( o2.path ) );
                                        if ( c != 0 ) {
                                            return c;
                                        }
                                        // Shorter path wins
                                        return Integer.compare( o1.path.stepCount(), o2.path.stepCount() );
                                    }


                                    private int worstKind( Path path ) {
                                        int kind = -1;
                                        for ( Step step : path.steps() ) {
                                            kind = Math.max( kind, step.kind.ordinal() );
                                        }
                                        return kind;
                                    }
                                };
                        resolved.resolves.sort( c );
                        if ( c.compare( resolved.resolves.get( 0 ), resolved.resolves.get( 1 ) ) == 0 ) {
                            throw validator.newValidationError( suffix, Static.RESOURCE.columnAmbiguous( suffix.toString() ) );
                        }
                        path = resolved.resolves.get( 0 ).path;
                }

                // Normalize case to match definition, make elided fields explicit, and check that references to dynamic stars ("**") are unambiguous.
                int k = i;
                for ( Step step : path.steps() ) {
                    final String name = identifier.names.get( k );
                    if ( step.i < 0 ) {
                        throw validator.newValidationError( identifier, Static.RESOURCE.columnNotFound( name ) );
                    }
                    final AlgDataTypeField field0 = step.rowType.getFieldList().get( step.i );
                    final String fieldName = field0.getName();
                    switch ( step.kind ) {
                        case PEEK_FIELDS:
                        case PEEK_FIELDS_DEFAULT:
                        case PEEK_FIELDS_NO_EXPAND:
                            identifier = identifier.add( k, fieldName, ParserPos.ZERO );
                            break;
                        default:
                            if ( !fieldName.equals( name ) ) {
                                identifier = identifier.setName( k, fieldName );
                            }
                            if ( hasAmbiguousUnresolvedStar( step.rowType, field0, name ) ) {
                                throw validator.newValidationError( identifier, Static.RESOURCE.columnAmbiguous( name ) );
                            }
                    }
                    ++k;
                }

                // Multiple name components may have been resolved as one step by CustomResolvingTable.
                if ( identifier.names.size() > k ) {
                    identifier = identifier.getComponent( 0, k );
                }

                if ( i > 1 ) {
                    // Simplify overqualified identifiers.
                    // For example, schema.emp.deptno becomes emp.deptno.
                    //
                    // It is safe to convert schema.emp or database.schema.emp to emp because it would not have resolved if the FROM item had an alias. The following query is invalid:
                    //   SELECT schema.emp.deptno FROM schema.emp AS e
                    identifier = identifier.getComponent( i - 1, identifier.names.size() );
                }

                if ( !previous.equals( identifier ) ) {
                    validator.setOriginal( identifier, previous );
                }
                return SqlQualified.create( this, i, fromNs, identifier );
            }
        }
    }


    @Override
    public void validateExpr( SqlNode expr ) {
        // Do not delegate to parent. An expression valid in this scope may not be valid in the parent scope.
    }


    @Override
    public SqlWindow lookupWindow( String name ) {
        return parent.lookupWindow( name );
    }


    @Override
    public Monotonicity getMonotonicity( SqlNode expr ) {
        return parent.getMonotonicity( expr );
    }


    @Override
    public SqlNodeList getOrderList() {
        return parent.getOrderList();
    }


    /**
     * Returns whether {@code rowType} contains more than one star column.
     * Having more than one star columns implies ambiguous column.
     */
    private boolean hasAmbiguousUnresolvedStar( AlgDataType rowType, AlgDataTypeField field, String columnName ) {
        if ( field.isDynamicStar() && !DynamicRecordType.isDynamicStarColName( columnName ) ) {
            int count = 0;
            for ( AlgDataTypeField possibleStar : rowType.getFieldList() ) {
                if ( possibleStar.isDynamicStar() ) {
                    if ( ++count > 1 ) {
                        return true;
                    }
                }
            }
        }
        return false;
    }


    /**
     * Returns the parent scope of this <code>DelegatingScope</code>.
     */
    public SqlValidatorScope getParent() {
        return parent;
    }

}
