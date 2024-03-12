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


import java.util.List;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.Pair;


/**
 * An implementation of {@link SqlValidatorNamespace} that delegates all methods to an underlying object.
 */
public abstract class DelegatingNamespace implements SqlValidatorNamespace {

    protected final SqlValidatorNamespace namespace;


    /**
     * Creates a DelegatingNamespace.
     *
     * @param namespace Underlying namespace, to delegate to
     */
    protected DelegatingNamespace( SqlValidatorNamespace namespace ) {
        this.namespace = namespace;
    }


    @Override
    public SqlValidator getValidator() {
        return namespace.getValidator();
    }


    @Override
    public Entity getEntity() {
        return namespace.getEntity();
    }


    @Override
    public AlgDataType getTupleType() {
        return namespace.getTupleType();
    }


    @Override
    public void setType( AlgDataType type ) {
        namespace.setType( type );
    }


    @Override
    public AlgDataType getRowTypeSansSystemColumns() {
        return namespace.getRowTypeSansSystemColumns();
    }


    @Override
    public AlgDataType getType() {
        return namespace.getType();
    }


    @Override
    public void validate( AlgDataType targetRowType ) {
        namespace.validate( targetRowType );
    }


    @Override
    public SqlNode getNode() {
        return namespace.getNode();
    }


    @Override
    public SqlNode getEnclosingNode() {
        return namespace.getEnclosingNode();
    }


    @Override
    public SqlValidatorNamespace lookupChild( String name ) {
        return namespace.lookupChild( name );
    }


    @Override
    public boolean fieldExists( String name ) {
        return namespace.fieldExists( name );
    }


    @Override
    public List<Pair<SqlNode, Monotonicity>> getMonotonicExprs() {
        return namespace.getMonotonicExprs();
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        return namespace.getMonotonicity( columnName );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        if ( clazz.isInstance( this ) ) {
            return clazz.cast( this );
        } else {
            return namespace.unwrap( clazz );
        }
    }


    @Override
    public boolean isWrapperFor( Class<?> clazz ) {
        return clazz.isInstance( this ) || namespace.isWrapperFor( clazz );
    }

}
