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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Deviant implementation of {@link SqlValidatorScope} for the top of the scope stack.
 *
 * It is convenient, because we never need to check whether a scope's parent is null. (This scope knows not to ask about its parents, just like Adam.)
 */
class EmptyScope implements SqlValidatorScope {

    protected final SqlValidatorImpl validator;


    EmptyScope( SqlValidatorImpl validator ) {
        this.validator = validator;
    }


    @Override
    public SqlValidator getValidator() {
        return validator;
    }


    @Override
    public SqlQualified fullyQualify( SqlIdentifier identifier ) {
        return SqlQualified.create( this, 1, null, identifier );
    }


    @Override
    public SqlNode getNode() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void resolve( List<String> names, NameMatcher nameMatcher, boolean deep, Resolved resolved ) {
    }


    @Override
    public SqlValidatorNamespace getTableNamespace( List<String> names ) {
        ValidatorTable table = validator.catalogReader.getTable( names );
        return table != null
                ? new TableNamespace( validator, table )
                : null;
    }


    @Override
    public void resolveTable( List<String> names, NameMatcher nameMatcher, Path path, Resolved resolved ) {
        final List<Resolve> imperfectResolves = new ArrayList<>();
        final List<Resolve> resolves = ((ResolvedImpl) resolved).resolves;

        // Look in the default schema, then default catalog, then root schema.
        for ( List<String> schemaPath : validator.catalogReader.getSchemaPaths() ) {
            resolve_( validator.catalogReader.getRootSchema(), names, schemaPath, nameMatcher, path, resolved );
            for ( Resolve resolve : resolves ) {
                if ( resolve.remainingNames.isEmpty() ) {
                    // There is a full match. Return it as the only match.
                    ((ResolvedImpl) resolved).clear();
                    resolves.add( resolve );
                    return;
                }
            }
            imperfectResolves.addAll( resolves );
        }
        // If there were no matches in the last round, restore those found in previous rounds
        if ( resolves.isEmpty() ) {
            resolves.addAll( imperfectResolves );
        }
    }


    // todo dl: refactor for 0.10
    private void resolve_( final PolyphenyDbSchema rootSchema, List<String> names, List<String> schemaNames, NameMatcher nameMatcher, Path path, Resolved resolved ) {
        final List<String> concat = ImmutableList.<String>builder().addAll( schemaNames ).addAll( names ).build();
        PolyphenyDbSchema schema = rootSchema;
        SqlValidatorNamespace namespace = null;
        List<String> remainingNames = concat;
        for ( String schemaName : concat ) {
            if ( schema == rootSchema && nameMatcher.matches( schemaName, schema.getName() ) ) {
                remainingNames = Util.skip( remainingNames );
                continue;
            }
            final PolyphenyDbSchema subSchema = schema.getSubSchema( schemaName, nameMatcher.isCaseSensitive() );
            if ( subSchema != null ) {
                path = path.plus( null, -1, subSchema.getName(), StructKind.NONE );
                remainingNames = Util.skip( remainingNames );
                schema = subSchema;
                namespace = new SchemaNamespace( validator, ImmutableList.copyOf( path.stepNames() ) );
                continue;
            }
            PolyphenyDbSchema.TableEntry entry = schema.getTable( schemaName );
            if ( entry == null ) {
                entry = schema.getTableBasedOnNullaryFunction( schemaName, nameMatcher.isCaseSensitive() );
            }
            if ( entry != null ) {
                path = path.plus( null, -1, entry.name, StructKind.NONE );
                remainingNames = Util.skip( remainingNames );
                final Table table = entry.getTable();
                ValidatorTable table2 = null;
                if ( table instanceof Wrapper ) {
                    table2 = ((Wrapper) table).unwrap( Prepare.PreparingTable.class );
                }
                if ( table2 == null ) {
                    final AlgOptSchema algOptSchema = validator.catalogReader.unwrap( AlgOptSchema.class );
                    final AlgDataType rowType = table.getRowType( validator.typeFactory );
                    table2 = AlgOptTableImpl.create( algOptSchema, rowType, entry, null );
                }
                namespace = new TableNamespace( validator, table2 );
                resolved.found( namespace, false, null, path, remainingNames );
                return;
            }
            // neither sub-schema nor table
            if ( namespace != null && !remainingNames.equals( names ) ) {
                resolved.found( namespace, false, null, path, remainingNames );
            }
            return;
        }
    }


    @Override
    public AlgDataType nullifyType( SqlNode node, AlgDataType type ) {
        return type;
    }


    @Override
    public void findAllColumnNames( List<Moniker> result ) {
    }


    public void findAllTableNames( List<Moniker> result ) {
    }


    @Override
    public void findAliases( Collection<Moniker> result ) {
    }


    @Override
    public AlgDataType resolveColumn( String name, SqlNode ctx ) {
        return null;
    }


    @Override
    public SqlValidatorScope getOperandScope( SqlCall call ) {
        return this;
    }


    @Override
    public void validateExpr( SqlNode expr ) {
        // valid
    }


    @Override
    public Pair<String, SqlValidatorNamespace> findQualifyingTableName( String columnName, SqlNode ctx ) {
        throw validator.newValidationError( ctx, Static.RESOURCE.columnNotFound( columnName ) );
    }


    @Override
    public Map<String, ScopeChild> findQualifyingTableNames( String columnName, SqlNode ctx, NameMatcher nameMatcher ) {
        return ImmutableMap.of();
    }


    @Override
    public void addChild( SqlValidatorNamespace ns, String alias, boolean nullable ) {
        // cannot add to the empty scope
        throw new UnsupportedOperationException();
    }


    @Override
    public SqlWindow lookupWindow( String name ) {
        // No windows defined in this scope.
        return null;
    }


    @Override
    public Monotonicity getMonotonicity( SqlNode expr ) {
        return ((expr instanceof SqlLiteral) || (expr instanceof SqlDynamicParam) || (expr instanceof SqlDataTypeSpec))
                ? Monotonicity.CONSTANT
                : Monotonicity.NOT_MONOTONIC;
    }


    @Override
    public SqlNodeList getOrderList() {
        // scope is not ordered
        return null;
    }

}

