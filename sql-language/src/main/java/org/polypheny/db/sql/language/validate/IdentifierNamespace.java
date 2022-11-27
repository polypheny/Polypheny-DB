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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.util.CyclicDefinitionException;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;


/**
 * Namespace whose contents are defined by the type of an {@link SqlIdentifier identifier}.
 */
public class IdentifierNamespace extends AbstractNamespace {

    private final SqlIdentifier id;
    private final SqlValidatorScope parentScope;
    public final SqlNodeList extendList;

    /**
     * The underlying namespace. Often a {@link TableNamespace}. Set on validate.
     */
    private SqlValidatorNamespace resolvedNamespace;

    /**
     * List of monotonic expressions. Set on validate.
     */
    private List<Pair<SqlNode, Monotonicity>> monotonicExprs;


    /**
     * Creates an IdentifierNamespace.
     *
     * @param validator Validator
     * @param id Identifier node (or "identifier EXTEND column-list")
     * @param extendList Extension columns, or null
     * @param enclosingNode Enclosing node
     * @param parentScope Parent scope which this namespace turns to in order to
     */
    IdentifierNamespace( SqlValidatorImpl validator, SqlIdentifier id, @Nullable SqlNodeList extendList, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
        super( validator, enclosingNode );
        this.id = id;
        this.extendList = extendList;
        this.parentScope = Objects.requireNonNull( parentScope );
    }


    IdentifierNamespace( SqlValidatorImpl validator, SqlNode node, SqlNode enclosingNode, SqlValidatorScope parentScope ) {
        this( validator, split( node ).left, split( node ).right, enclosingNode, parentScope );
    }


    protected static Pair<SqlIdentifier, SqlNodeList> split( SqlNode node ) {
        switch ( node.getKind() ) {
            case EXTEND:
                final SqlCall call = (SqlCall) node;
                return Pair.of( (SqlIdentifier) call.getOperandList().get( 0 ), (SqlNodeList) call.getOperandList().get( 1 ) );
            default:
                return Pair.of( (SqlIdentifier) node, null );
        }
    }


    private SqlValidatorNamespace resolveImpl( SqlIdentifier id ) {
        final NameMatcher nameMatcher = validator.catalogReader.nameMatcher();
        final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
        final List<String> names = SqlIdentifier.toStar( id.names );
        try {
            parentScope.resolveTable( names, nameMatcher, SqlValidatorScope.Path.EMPTY, resolved );
        } catch ( CyclicDefinitionException e ) {
            if ( e.depth == 1 ) {
                throw validator.newValidationError( id, Static.RESOURCE.cyclicDefinition( id.toString(), SqlIdentifier.getString( e.path ) ) );
            } else {
                throw new CyclicDefinitionException( e.depth - 1, e.path );
            }
        }
        SqlValidatorScope.Resolve previousResolve = null;
        if ( resolved.count() == 1 ) {
            final SqlValidatorScope.Resolve resolve = previousResolve = resolved.only();
            if ( resolve.remainingNames.isEmpty() ) {
                return resolve.namespace;
            }
            // If we're not case-sensitive, give an error.
            // If we're case-sensitive, we'll shortly try again and give an error then.
            if ( !nameMatcher.isCaseSensitive() ) {
                throw validator.newValidationError( id, Static.RESOURCE.objectNotFoundWithin( resolve.remainingNames.get( 0 ), SqlIdentifier.getString( resolve.path.stepNames() ) ) );
            }
        }

        // Failed to match.  If we're matching case-sensitively, try a more lenient match. If we find something we can offer a helpful hint.
        if ( nameMatcher.isCaseSensitive() ) {
            final NameMatcher liberalMatcher = NameMatchers.liberal();
            resolved.clear();
            parentScope.resolveTable( names, liberalMatcher, SqlValidatorScope.Path.EMPTY, resolved );
            if ( resolved.count() == 1 ) {
                final SqlValidatorScope.Resolve resolve = resolved.only();
                if ( resolve.remainingNames.isEmpty() || previousResolve == null ) {
                    // We didn't match it case-sensitive, so they must have had the right identifier, wrong case.
                    //
                    // If previousResolve is null, we matched nothing case-sensitive and everything case-insensitive, so the mismatch must have been at position 0.
                    final int i = previousResolve == null
                            ? 0
                            : previousResolve.path.stepCount();
                    final int offset = resolve.path.stepCount() + resolve.remainingNames.size() - names.size();
                    final List<String> prefix = resolve.path.stepNames().subList( 0, offset + i );
                    final String next = resolve.path.stepNames().get( i + offset );
                    if ( prefix.isEmpty() ) {
                        throw validator.newValidationError( id, Static.RESOURCE.objectNotFoundDidYouMean( names.get( i ), next ) );
                    } else {
                        throw validator.newValidationError( id, Static.RESOURCE.objectNotFoundWithinDidYouMean( names.get( i ), SqlIdentifier.getString( prefix ), next ) );
                    }
                } else {
                    throw validator.newValidationError( id, Static.RESOURCE.objectNotFoundWithin( resolve.remainingNames.get( 0 ), SqlIdentifier.getString( resolve.path.stepNames() ) ) );
                }
            }
        }
        throw validator.newValidationError( id, Static.RESOURCE.objectNotFound( id.getComponent( 0 ).toString() ) );
    }


    @Override
    public AlgDataType validateImpl( AlgDataType targetRowType ) {
        resolvedNamespace = Objects.requireNonNull( resolveImpl( id ) );
        if ( resolvedNamespace instanceof TableNamespace ) {
            ValidatorTable table = resolvedNamespace.getTable();
            if ( validator.shouldExpandIdentifiers() ) {
                // TODO:  expand qualifiers for column references also
                List<String> qualifiedNames = table.getQualifiedName();
                if ( qualifiedNames != null ) {
                    // Assign positions to the components of the fully-qualified identifier, as best we can. We assume that qualification adds names to the front, e.g. FOO.BAR becomes BAZ.FOO.BAR.
                    List<ParserPos> poses = new ArrayList<>( Collections.nCopies( qualifiedNames.size(), id.getPos() ) );
                    int offset = qualifiedNames.size() - id.names.size();

                    // Test offset in case catalog supports fewer qualifiers than catalog reader.
                    if ( offset >= 0 ) {
                        for ( int i = 0; i < id.names.size(); i++ ) {
                            poses.set( i + offset, id.getComponentParserPosition( i ) );
                        }
                    }
                    id.setNames( qualifiedNames, poses );
                }
            }
        }

        AlgDataType rowType = resolvedNamespace.getRowType();

        if ( extendList != null ) {
            if ( !(resolvedNamespace instanceof TableNamespace) ) {
                throw new RuntimeException( "cannot convert" );
            }
            resolvedNamespace = ((TableNamespace) resolvedNamespace).extend( extendList );
            rowType = resolvedNamespace.getRowType();
        }

        // Build a list of monotonic expressions.
        final ImmutableList.Builder<Pair<SqlNode, Monotonicity>> builder = ImmutableList.builder();
        List<AlgDataTypeField> fields = rowType.getFieldList();
        for ( AlgDataTypeField field : fields ) {
            final String fieldName = field.getName();
            final Monotonicity monotonicity = resolvedNamespace.getMonotonicity( fieldName );
            if ( monotonicity != Monotonicity.NOT_MONOTONIC ) {
                builder.add( Pair.of( (SqlNode) new SqlIdentifier( fieldName, ParserPos.ZERO ), monotonicity ) );
            }
        }
        monotonicExprs = builder.build();

        // Validation successful.
        return rowType;
    }


    public SqlIdentifier getId() {
        return id;
    }


    @Override
    public SqlNode getNode() {
        return id;
    }


    @Override
    public SqlValidatorNamespace resolve() {
        assert resolvedNamespace != null : "must call validate first";
        return resolvedNamespace.resolve();
    }


    @Override
    public ValidatorTable getTable() {
        return resolvedNamespace == null ? null : resolve().getTable();
    }


    @Override
    public List<Pair<SqlNode, Monotonicity>> getMonotonicExprs() {
        return monotonicExprs;
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        final ValidatorTable table = getTable();
        return table.getMonotonicity( columnName );
    }


    @Override
    public boolean supportsModality( Modality modality ) {
        final ValidatorTable table = getTable();
        if ( table == null ) {
            return modality == Modality.RELATION;
        }
        return table.supportsModality( modality );
    }

}

