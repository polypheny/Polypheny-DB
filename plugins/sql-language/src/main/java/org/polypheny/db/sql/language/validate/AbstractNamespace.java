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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Abstract implementation of {@link SqlValidatorNamespace}.
 */
abstract class AbstractNamespace implements SqlValidatorNamespace {

    protected final SqlValidatorImpl validator;


    /**
     * Whether this scope is currently being validated. Used to check for cycles.
     */
    private SqlValidatorImpl.Status status = SqlValidatorImpl.Status.UNVALIDATED;


    /**
     * Type of the output row, which comprises the name and type of each output column. Set on validate.
     */
    protected AlgDataType rowType;


    /**
     * As {@link #rowType}, but not necessarily a struct.
     */
    protected AlgDataType type;

    protected final SqlNode enclosingNode;


    /**
     * Creates an AbstractNamespace.
     *
     * @param validator Validator
     * @param enclosingNode Enclosing node
     */
    AbstractNamespace( SqlValidatorImpl validator, SqlNode enclosingNode ) {
        this.validator = validator;
        this.enclosingNode = enclosingNode;
    }


    @Override
    public SqlValidator getValidator() {
        return validator;
    }


    @Override
    public final void validate( AlgDataType targetTupleType ) {
        switch ( status ) {
            case UNVALIDATED:
                try {
                    status = SqlValidatorImpl.Status.IN_PROGRESS;
                    Preconditions.checkArgument( rowType == null, "Namespace.rowType must be null before validate has been called" );
                    AlgDataType type = validateImpl( targetTupleType );
                    Preconditions.checkArgument( type != null, "validateImpl() returned null" );
                    setType( type );
                } finally {
                    status = SqlValidatorImpl.Status.VALID;
                }
                break;
            case IN_PROGRESS:
                throw new AssertionError( "Cycle detected during type-checking" );
            case VALID:
                break;
            default:
                throw Util.unexpected( status );
        }
    }


    /**
     * Validates this scope and returns the type of the records it returns.
     * External users should call {@link #validate}, which uses the {@link #status} field to protect against cycles.
     *
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     * @return record data type, never null
     */
    protected abstract AlgDataType validateImpl( AlgDataType targetRowType );


    @Override
    public AlgDataType getTupleType() {
        if ( rowType instanceof GraphType ) {
            return GraphType.ofRelational();
        }
        if ( rowType instanceof DocumentType ) {
            return DocumentType.ofCrossRelational();
        }
        if ( rowType == null ) {
            validator.validateNamespace( this, validator.unknownType );
            Preconditions.checkArgument( rowType != null, "validate must set rowType" );
        }
        return rowType;
    }


    @Override
    public AlgDataType getRowTypeSansSystemColumns() {
        return getTupleType();
    }


    @Override
    public AlgDataType getType() {
        Util.discard( getTupleType() );
        return type;
    }


    @Override
    public void setType( AlgDataType type ) {
        this.type = type;
        this.rowType = convertToStruct( type );
    }


    @Override
    public SqlNode getEnclosingNode() {
        return enclosingNode;
    }


    @Override
    public Entity getEntity() {
        return null;
    }


    @Override
    public SqlValidatorNamespace lookupChild( String name ) {
        return validator.lookupFieldNamespace( getTupleType(), name );
    }


    @Override
    public boolean fieldExists( String name ) {
        final AlgDataType rowType = getTupleType();
        return NameMatchers.withCaseSensitive( false ).field( rowType, name ) != null;
    }


    @Override
    public List<Pair<SqlNode, Monotonicity>> getMonotonicExprs() {
        return ImmutableList.of();
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        return Monotonicity.NOT_MONOTONIC;
    }


    public String translate( String name ) {
        return name;
    }


    @Override
    public SqlValidatorNamespace resolve() {
        return this;
    }


    @Override
    public boolean supportsModality( Modality modality ) {
        return true;
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        return clazz.cast( this );
    }


    @Override
    public boolean isWrapperFor( Class<?> clazz ) {
        return clazz.isInstance( this );
    }


    protected AlgDataType convertToStruct( AlgDataType type ) {
        // "MULTISET [<expr>, ...]" needs to be wrapped in a record if <expr> has a scalar type.
        // For example, "MULTISET [8, 9]" has type "RECORD(INTEGER EXPR$0 NOT NULL) NOT NULL MULTISET NOT NULL".
        final AlgDataType componentType = type.getComponentType();
        if ( componentType == null || componentType.isStruct() ) {
            return type;
        }
        final AlgDataTypeFactory typeFactory = validator.getTypeFactory();
        final AlgDataType structType = toStruct( componentType, getNode() );
        final AlgDataType collectionType;
        switch ( type.getPolyType() ) {
            case ARRAY:
                collectionType = typeFactory.createArrayType( structType, -1 );
                break;
            case MULTISET:
                collectionType = typeFactory.createMultisetType( structType, -1 );
                break;
            default:
                throw new AssertionError( type );
        }
        return typeFactory.createTypeWithNullability( collectionType, type.isNullable() );
    }


    /**
     * Converts a type to a struct if it is not already.
     */
    protected AlgDataType toStruct( AlgDataType type, SqlNode unnest ) {
        if ( type.isStruct() ) {
            return type;
        }
        return validator.getTypeFactory().builder()
                .add( null, validator.deriveAlias( unnest, 0 ), null, type )
                .build();
    }

}

