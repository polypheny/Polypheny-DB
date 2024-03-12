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


import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
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


/**
 * Deviant implementation of {@link SqlValidatorScope} for the top of the scope stack.
 * <p>
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
    public void resolve( List<String> names, boolean deep, Resolved resolved ) {
    }


    @Override
    public SqlValidatorNamespace getEntityNamespace( List<String> names ) {
        Entity table = validator.snapshot.rel().getTable( names.get( 0 ), names.get( 1 ) ).orElse( null );
        return table != null
                ? new EntityNamespace( validator, table )
                : null;
    }


    @Override
    public void resolveEntity( List<String> names, Path path, Resolved resolved ) {
        LogicalEntity entity;
        if ( names.size() == 3 ) {
            entity = validator.snapshot.rel().getTable( names.get( 1 ), names.get( 2 ) ).orElse( null );
        } else if ( names.size() == 2 ) {
            entity = validator.snapshot.rel().getTable( names.get( 0 ), names.get( 1 ) ).orElse( null );
        } else if ( names.size() == 1 ) {
            entity = validator.snapshot.rel().getTable( Catalog.defaultNamespaceId, names.get( 0 ) ).orElse( null );
        } else {
            throw new GenericRuntimeException( "Table is not known" );
        }

        if ( entity != null ) {
            resolved.found( new EntityNamespace( validator, entity ), false, null, Path.EMPTY, List.of() );
        }

    }



    @Override
    public AlgDataType nullifyType( SqlNode node, AlgDataType type ) {
        return type;
    }


    @Override
    public void findAllColumnNames( List<Moniker> result ) {
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
    public Pair<String, SqlValidatorNamespace> findQualifyingEntityName( String columnName, SqlNode ctx ) {
        throw validator.newValidationError( ctx, Static.RESOURCE.fieldNotFound( columnName ) );
    }


    @Override
    public Map<String, ScopeChild> findQualifyingEntityNames( String columnName, SqlNode ctx, NameMatcher nameMatcher ) {
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

