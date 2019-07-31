/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.prepare.RelOptTableImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.StructKind;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDynamicParam;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWindow;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


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


    public SqlValidator getValidator() {
        return validator;
    }


    public SqlQualified fullyQualify( SqlIdentifier identifier ) {
        return SqlQualified.create( this, 1, null, identifier );
    }


    public SqlNode getNode() {
        throw new UnsupportedOperationException();
    }


    public void resolve( List<String> names, SqlNameMatcher nameMatcher, boolean deep, Resolved resolved ) {
    }


    @SuppressWarnings("deprecation")
    public SqlValidatorNamespace getTableNamespace( List<String> names ) {
        SqlValidatorTable table = validator.catalogReader.getTable( names );
        return table != null
                ? new TableNamespace( validator, table )
                : null;
    }


    public void resolveTable( List<String> names, SqlNameMatcher nameMatcher, Path path, Resolved resolved ) {
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


    private void resolve_( final PolyphenyDbSchema rootSchema, List<String> names, List<String> schemaNames, SqlNameMatcher nameMatcher, Path path, Resolved resolved ) {
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
            PolyphenyDbSchema.TableEntry entry = schema.getTable( schemaName, nameMatcher.isCaseSensitive() );
            if ( entry == null ) {
                entry = schema.getTableBasedOnNullaryFunction( schemaName, nameMatcher.isCaseSensitive() );
            }
            if ( entry != null ) {
                path = path.plus( null, -1, entry.name, StructKind.NONE );
                remainingNames = Util.skip( remainingNames );
                final Table table = entry.getTable();
                SqlValidatorTable table2 = null;
                if ( table instanceof Wrapper ) {
                    table2 = ((Wrapper) table).unwrap( Prepare.PreparingTable.class );
                }
                if ( table2 == null ) {
                    final RelOptSchema relOptSchema = validator.catalogReader.unwrap( RelOptSchema.class );
                    final RelDataType rowType = table.getRowType( validator.typeFactory );
                    table2 = RelOptTableImpl.create( relOptSchema, rowType, entry, null );
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


    public RelDataType nullifyType( SqlNode node, RelDataType type ) {
        return type;
    }


    public void findAllColumnNames( List<SqlMoniker> result ) {
    }


    public void findAllTableNames( List<SqlMoniker> result ) {
    }


    public void findAliases( Collection<SqlMoniker> result ) {
    }


    public RelDataType resolveColumn( String name, SqlNode ctx ) {
        return null;
    }


    public SqlValidatorScope getOperandScope( SqlCall call ) {
        return this;
    }


    public void validateExpr( SqlNode expr ) {
        // valid
    }


    @SuppressWarnings("deprecation")
    public Pair<String, SqlValidatorNamespace> findQualifyingTableName( String columnName, SqlNode ctx ) {
        throw validator.newValidationError( ctx, Static.RESOURCE.columnNotFound( columnName ) );
    }


    public Map<String, ScopeChild> findQualifyingTableNames( String columnName, SqlNode ctx, SqlNameMatcher nameMatcher ) {
        return ImmutableMap.of();
    }


    public void addChild( SqlValidatorNamespace ns, String alias, boolean nullable ) {
        // cannot add to the empty scope
        throw new UnsupportedOperationException();
    }


    public SqlWindow lookupWindow( String name ) {
        // No windows defined in this scope.
        return null;
    }


    public SqlMonotonicity getMonotonicity( SqlNode expr ) {
        return ((expr instanceof SqlLiteral) || (expr instanceof SqlDynamicParam) || (expr instanceof SqlDataTypeSpec))
                ? SqlMonotonicity.CONSTANT
                : SqlMonotonicity.NOT_MONOTONIC;
    }


    public SqlNodeList getOrderList() {
        // scope is not ordered
        return null;
    }
}

