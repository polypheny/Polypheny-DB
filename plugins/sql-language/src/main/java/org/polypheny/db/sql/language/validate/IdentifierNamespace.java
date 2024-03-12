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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph.SubstitutionGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.CyclicDefinitionException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Namespace whose contents are defined by the type of an {@link SqlIdentifier identifier}.
 */
@Slf4j
public class IdentifierNamespace extends AbstractNamespace {

    @Getter
    private final SqlIdentifier id;
    private final SqlValidatorScope parentScope;
    public final SqlNodeList extendList;

    @Getter
    @Accessors(fluent = true)
    public DataModel dataModel = DataModel.RELATIONAL;

    /**
     * The underlying namespace. Often a {@link EntityNamespace}. Set on validate.
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
        if ( Objects.requireNonNull( node.getKind() ) == Kind.EXTEND ) {
            final SqlCall call = (SqlCall) node;
            return Pair.of( (SqlIdentifier) call.getOperandList().get( 0 ), (SqlNodeList) call.getOperandList().get( 1 ) );
        }
        return Pair.of( (SqlIdentifier) node, null );
    }


    private SqlValidatorNamespace resolveImpl( SqlIdentifier id ) {
        final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
        final List<String> names = SqlIdentifier.toStar( id.names );
        try {
            parentScope.resolveEntity( names, SqlValidatorScope.Path.EMPTY, resolved );
        } catch ( CyclicDefinitionException e ) {
            if ( e.depth == 1 ) {
                throw validator.newValidationError( id, Static.RESOURCE.cyclicDefinition( id.toString(), SqlIdentifier.getString( e.path ) ) );
            } else {
                throw new CyclicDefinitionException( e.depth - 1, e.path );
            }
        }
        if ( resolved.count() == 1 ) {
            final SqlValidatorScope.Resolve resolve = resolved.only();
            return resolve.namespace;
        }

        List<String> ns = id.names;

        if ( ns.size() == 1 ) {
            List<LogicalTable> tables = validator.snapshot.rel().getTables( Catalog.defaultNamespaceId, Pattern.of( ns.get( 0 ) ) );
            if ( tables.size() > 1 ) {
                throw new GenericRuntimeException( "Too many tables" );
            } else if ( tables.size() == 1 ) {
                return new EntityNamespace( validator, tables.get( 0 ) );
            } else if ( !validator.snapshot.rel().getTables( null, Pattern.of( ns.get( 0 ) ) ).isEmpty() ) {
                return new EntityNamespace( validator, validator.snapshot.rel().getTables( null, Pattern.of( ns.get( 0 ) ) ).get( 0 ) );
            }
        } else if ( ns.size() == 2 ) {
            String entityName = ns.get( 1 );

            Optional<LogicalNamespace> optionalNamespace = validator.snapshot.getNamespace( ns.get( 0 ) );
            if ( optionalNamespace.isEmpty() ) {
                // we might have [entity].[column] not [namespace].[entity]
                optionalNamespace = validator.snapshot.getNamespace( Catalog.defaultNamespaceId );
                entityName = ns.get( 0 );
            }

            LogicalNamespace namespace = optionalNamespace.orElseThrow();

            Entity entity = null;
            if ( namespace.dataModel == DataModel.RELATIONAL ) {
                entity = validator.snapshot.rel().getTable( namespace.id, entityName ).orElse( null );
            } else if ( namespace.dataModel == DataModel.DOCUMENT ) {
                entity = validator.snapshot.doc().getCollection( namespace.id, entityName ).orElse( null );
            } else if ( namespace.dataModel == DataModel.GRAPH ) {
                // we use a subgraph to define label which is used as table
                final String finalEntityName = entityName;
                entity = validator.snapshot.graph().getGraph( namespace.id ).map( g -> new SubstitutionGraph( g.id, ns.get( 1 ), false, g.caseSensitive, List.of( PolyString.of( finalEntityName ) ) ) ).orElse( null );
            }

            return new EntityNamespace( validator, entity );
        }
        throw new GenericRuntimeException( "Entity not found" );
    }


    @Override
    public AlgDataType validateImpl( AlgDataType targetRowType ) {
        resolvedNamespace = Objects.requireNonNull( resolveImpl( id ) );
        if ( resolvedNamespace instanceof EntityNamespace ) {
            Entity table = resolvedNamespace.getEntity();
            if ( validator.shouldExpandIdentifiers() ) {
                // TODO:  expand qualifiers for column references also
                List<String> qualifiedNames = List.of( table.name );
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

        AlgDataType rowType = resolvedNamespace.getTupleType();

        if ( extendList != null ) {
            if ( !(resolvedNamespace instanceof EntityNamespace) ) {
                throw new GenericRuntimeException( "cannot convert" );
            }
            resolvedNamespace = ((EntityNamespace) resolvedNamespace).extend( extendList );
            rowType = resolvedNamespace.getTupleType();
        }

        // Build a list of monotonic expressions.
        final ImmutableList.Builder<Pair<SqlNode, Monotonicity>> builder = ImmutableList.builder();
        List<AlgDataTypeField> fields = rowType.getFields();
        for ( AlgDataTypeField field : fields ) {
            final String fieldName = field.getName();
            final Monotonicity monotonicity = resolvedNamespace.getMonotonicity( fieldName );
            if ( monotonicity != Monotonicity.NOT_MONOTONIC ) {
                builder.add( Pair.of( new SqlIdentifier( fieldName, ParserPos.ZERO ), monotonicity ) );
            }
        }
        monotonicExprs = builder.build();

        // Validation successful.
        return rowType;
    }


    @Override
    public SqlNode getNode() {
        return id;
    }


    @Override
    public SqlValidatorNamespace resolve() {
        if ( resolvedNamespace == null ) {
            log.warn( "resolvedNamespace is null" );
        }
        assert resolvedNamespace != null : "must call validate first";
        return resolvedNamespace.resolve();
    }


    @Override
    public Entity getEntity() {
        return resolvedNamespace == null ? null : resolve().getEntity();
    }


    @Override
    public List<Pair<SqlNode, Monotonicity>> getMonotonicExprs() {
        return monotonicExprs;
    }


    @Override
    public Monotonicity getMonotonicity( String columnName ) {
        final Entity table = this.getEntity();
        return Util.getMonotonicity( table, columnName );
    }


    @Override
    public boolean supportsModality( Modality modality ) {
        final Entity table = this.getEntity();
        if ( table == null ) {
            return modality == Modality.RELATION;
        }

        return SqlUtil.supportsModality( modality, table );
    }

}

