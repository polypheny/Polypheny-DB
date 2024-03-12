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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.constant.MonikerType;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.util.Moniker;
import org.polypheny.db.util.MonikerImpl;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NameMatchers;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Abstract base for a scope which is defined by a list of child namespaces and which inherits from a parent scope.
 */
public abstract class ListScope extends DelegatingScope {

    /**
     * List of child {@link SqlValidatorNamespace} objects and their names.
     */
    public final List<ScopeChild> children = new ArrayList<>();


    public ListScope( SqlValidatorScope parent ) {
        super( parent );
    }


    @Override
    public void addChild( SqlValidatorNamespace ns, String alias, boolean nullable ) {
        Objects.requireNonNull( alias );
        children.add( new ScopeChild( children.size(), alias, ns, nullable ) );
    }


    /**
     * Returns an immutable list of child namespaces.
     *
     * @return list of child namespaces
     */
    public List<SqlValidatorNamespace> getChildren() {
        return children.stream().map( scopeChild -> scopeChild.namespace ).collect( Collectors.toList() );
    }


    /**
     * Returns an immutable list of child names.
     *
     * @return list of child namespaces
     */
    List<String> getChildNames() {
        return children.stream().map( scopeChild -> scopeChild.name ).collect( Collectors.toList() );
    }


    private ScopeChild findChild( List<String> names ) {
        for ( ScopeChild child : children ) {
            if ( child.namespace.getEntity() != null && child.namespace.resolve().getDataModel() != DataModel.RELATIONAL ) {
                return child;
            }
            NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
            if ( child.namespace.getEntity() != null ) {
                // we do not yet know the parent namespace, so we cannot resolve the casing, we are liberal and match case-insensitive
                nameMatcher = NameMatchers.withCaseSensitive( Catalog.snapshot().getNamespace( child.namespace.getEntity().namespaceId ).orElseThrow().caseSensitive );
            }

            String lastName = Util.last( names );
            if ( child.name != null ) {
                if ( !nameMatcher.matches( child.name, lastName ) ) {
                    // Alias does not match last segment. Don't consider the fully-qualified name. E.g.
                    //    SELECT sales.emp.name FROM sales.emp AS otherAlias
                    continue;
                }
                if ( names.size() == 1 ) {
                    return child;
                }
            }

            // Look up the 2 tables independently, in case one is qualified with catalog & schema and the other is not.
            final Entity table = child.namespace.getEntity();
            if ( table != null ) {
                Optional<? extends LogicalEntity> optionalEntity = getEntity( names );
                if ( optionalEntity.isPresent()
                        && optionalEntity.get().name.equals( table.name )
                        && optionalEntity.get().namespaceId == table.namespaceId ) {
                    return child;
                }
            }
        }
        return null;
    }


    private Optional<? extends org.polypheny.db.catalog.entity.logical.LogicalEntity> getEntity( List<String> names ) {
        if ( names.size() == 2 ) {
            return validator.snapshot.rel().getTable( names.get( 0 ), names.get( 1 ) );
        } else if ( names.size() == 1 ) {
            return validator.snapshot.rel().getTable( Catalog.defaultNamespaceId, names.get( 0 ) );
        } else {
            throw new GenericRuntimeException( "Table is not known" );
        }
    }


    @Override
    public void findAllColumnNames( List<Moniker> result ) {
        for ( ScopeChild child : children ) {
            addColumnNames( child.namespace, result );
        }
        parent.findAllColumnNames( result );
    }


    @Override
    public void findAliases( Collection<Moniker> result ) {
        for ( ScopeChild child : children ) {
            result.add( new MonikerImpl( child.name, MonikerType.TABLE ) );
        }
        parent.findAliases( result );
    }


    @Override
    public Pair<String, SqlValidatorNamespace> findQualifyingEntityName( final String columnName, SqlNode ctx ) {
        final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
        final Map<String, ScopeChild> map = findQualifyingEntityNames( columnName, ctx, nameMatcher );
        return switch ( map.size() ) {
            case 0 -> throw validator.newValidationError( ctx, Static.RESOURCE.fieldNotFound( columnName ) );
            case 1 -> {
                final Map.Entry<String, ScopeChild> entry = map.entrySet().iterator().next();
                yield Pair.of( entry.getKey(), entry.getValue().namespace );
            }
            default -> throw validator.newValidationError( ctx, Static.RESOURCE.columnAmbiguous( columnName ) );
        };
    }


    @Override
    public Map<String, ScopeChild> findQualifyingEntityNames( String columnName, SqlNode ctx, NameMatcher nameMatcher ) {
        final Map<String, ScopeChild> map = new HashMap<>();
        for ( ScopeChild child : children ) {
            final ResolvedImpl resolved = new ResolvedImpl();
            resolve( ImmutableList.of( child.name, columnName ), true, resolved );
            if ( resolved.count() > 0 ) {
                map.put( child.name, child );
            }
        }
        if ( map.isEmpty() ) {
            return parent.findQualifyingEntityNames( columnName, ctx, nameMatcher );
        }
        return map;
    }


    @Override
    public void resolve( List<String> names, boolean deep, Resolved resolved ) {
        // First resolve by looking through the child namespaces.
        final ScopeChild child0 = findChild( names );

        if ( child0 != null ) {
            final Step path =
                    Path.EMPTY.plus(
                            child0.namespace.getTupleType(),
                            child0.ordinal,
                            child0.name,
                            StructKind.FULLY_QUALIFIED );

            resolved.found(
                    child0.namespace,
                    child0.nullable,
                    this,
                    path,
                    null );
            return;
        }
        NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );

        // Recursively look deeper into the record-valued fields of the namespace, if it allows skipping fields.
        if ( deep ) {
            for ( ScopeChild child : children ) {
                // If identifier starts with table alias, remove the alias.
                final List<String> names2 =
                        nameMatcher.matches( child.name, names.get( 0 ) )
                                ? names.subList( 1, names.size() )
                                : names;
                resolveInNamespace(
                        child.namespace,
                        child.nullable,
                        names2,
                        nameMatcher,
                        Path.EMPTY, resolved );
            }
            if ( resolved.count() > 0 ) {
                return;
            }
        }

        // Then call the base class method, which will delegate to the parent scope.
        super.resolve( names, deep, resolved );
    }


    @Override
    public AlgDataType resolveColumn( String columnName, SqlNode ctx ) {
        final NameMatcher nameMatcher = NameMatchers.withCaseSensitive( false );
        int found = 0;
        AlgDataType type = null;
        for ( ScopeChild child : children ) {
            SqlValidatorNamespace childNs = child.namespace;
            final AlgDataType childRowType = childNs.getTupleType();
            final AlgDataTypeField field = nameMatcher.field( childRowType, columnName );
            if ( field != null ) {
                found++;
                type = field.getType();
            }
        }
        return switch ( found ) {
            case 0 -> null;
            case 1 -> type;
            default -> throw validator.newValidationError( ctx, Static.RESOURCE.columnAmbiguous( columnName ) );
        };
    }

}
