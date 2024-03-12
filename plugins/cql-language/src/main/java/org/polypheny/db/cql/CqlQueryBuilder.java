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

package org.polypheny.db.cql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cql.BooleanGroup.EntityOpsBooleanOperator;
import org.polypheny.db.cql.BooleanGroup.FieldOpsBooleanOperator;
import org.polypheny.db.cql.exception.InvalidModifierException;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.utils.Tree;
import org.polypheny.db.util.Pair;


/**
 * Packaging all the information and algorithm used by
 * {@code org.polypheny.db.cql.parser.CqlParser} to build {@link CqlQuery}.
 */
@Slf4j
public class CqlQueryBuilder {

    private final Stack<Tree<BooleanGroup<FieldOpsBooleanOperator>, Filter>> filters;
    private final Map<String, EntityIndex> entityIndexMapping;
    private final Map<String, FieldIndex> fieldIndexMapping;
    private final List<Pair<FieldIndex, Map<String, Modifier>>> sortSpecifications;
    private final Projections projections;
    private Tree<Combiner, EntityIndex> queryRelation;
    private EntityIndex lastEntityIndex;


    public CqlQueryBuilder( String namespace ) {
        this.filters = new Stack<>();
        this.entityIndexMapping = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.fieldIndexMapping = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
        this.sortSpecifications = new ArrayList<>();
        this.projections = new Projections();
    }


    /**
     * Build the {@link CqlQuery} object. To be called after
     * the {@code org.polypheny.db.cql.parser.CqlParser} has parsed
     * the full query.
     *
     * @return {@link CqlQuery}
     * @throws Exception Thrown if the query specified no relation or filters
     * OR if the filters stack has more than one filter trees.
     */
    public CqlQuery build() throws Exception {
        log.debug( "Building CqlQuery." );
        if ( queryRelation == null && filters.isEmpty() ) {
            log.error( "Query relations and filters cannot both be empty." );
            throw new Exception( "Query relations and filters cannot both be empty." );
        }

        if ( queryRelation == null ) {
            assert filters.size() == 1;
            log.debug( "QueryRelation not specified." );
            generateDefaultQueryRelation();
        }

        Tree<BooleanGroup<FieldOpsBooleanOperator>, Filter> queryFilters = null;
        if ( filters.size() == 1 ) {
            log.debug( "Found filters." );
            queryFilters = filters.pop();
        } else if ( filters.size() > 1 ) {
            log.error( "Invalid filters stack state. It should have exactly one or zero filter trees." );
            throw new Exception( "Invalid filters stack state. It should have exactly one or zero filter trees." );
        }

        return new CqlQuery(
                queryRelation,
                queryFilters,
                entityIndexMapping,
                fieldIndexMapping,
                sortSpecifications,
                projections );
    }


    /**
     * Generates a default relation made by of INNER JOIN-ing the entitys
     * of all the fields (including fields in filters, sort specifications
     * and projections) used in the query.
     */
    private void generateDefaultQueryRelation() {
        log.debug( "Generating Default Query Relation." );
        AtomicBoolean first = new AtomicBoolean( true );
        entityIndexMapping.forEach( ( entityName, entityIndex ) -> {
            if ( first.get() ) {
                addEntity( entityIndex );
                first.set( false );
            } else {
                BooleanGroup<EntityOpsBooleanOperator> innerJoin = new BooleanGroup<>( EntityOpsBooleanOperator.AND );
                try {
                    combineRelationWith( entityIndex, innerJoin );
                } catch ( InvalidModifierException e ) {
                    log.error( "Exception Unexpected.", e );
                    throw new GenericRuntimeException( "This exception will never be throws since the BooleanGroup used has no modifiers." );
                }
            }
        } );
    }


    /**
     * Creates and adds a {@link EntityIndex} as represented by the
     * input parameter.
     *
     * @param fullyQualifiedEntityName Expected format: NAMESPACE_NAME.ENTITY_NAME.
     * @return {@link EntityIndex}.
     * @throws UnknownIndexException Thrown if the {@link org.polypheny.db.catalog.Catalog}
     * does not contain the entity as specified by the input parameter.
     */
    public EntityIndex addEntityIndex( String fullyQualifiedEntityName ) throws UnknownIndexException {
        String[] split = fullyQualifiedEntityName.split( "\\." );

        assert split.length == 2;

        return addEntityIndex( split[0], split[1] );
    }


    /**
     * Creates and adds a {@link EntityIndex} as represented by the
     * input parameters.
     *
     * @return {@link EntityIndex}.
     */
    public EntityIndex addEntityIndex( String namespaceName, String entityName ) {
        String fullyQualifiedEntityName = namespaceName + "." + entityName;

        if ( !this.entityIndexMapping.containsKey( fullyQualifiedEntityName ) ) {
            EntityIndex entityIndex = EntityIndex.createIndex( namespaceName, entityName );
            this.entityIndexMapping.put( entityIndex.fullyQualifiedName, entityIndex );
        }
        return this.entityIndexMapping.get( fullyQualifiedEntityName );
    }


    /**
     * Creates and adds a {@link FieldIndex} as represented by the
     * input parameter. It also adds the {@link EntityIndex} of the
     * entity that the field belongs to.
     *
     * @param fullyQualifiedFieldName Expected format: NAMESPACE_NAME.NAMESPACE_NAME.field_NAME.
     * @return {@link FieldIndex}.
     * @throws UnknownIndexException Thrown if the {@link org.polypheny.db.catalog.Catalog}
     * does not contain the Field as specified by the input parameter.
     */
    public FieldIndex addFieldIndex( String fullyQualifiedFieldName ) throws UnknownIndexException {
        String[] split = fullyQualifiedFieldName.split( "\\." );
        assert split.length == 3;
        return addFieldIndex( split[0], split[1], split[2] );
    }


    /**
     * Creates and adds a {@link FieldIndex} as represented by the
     * input parameter. It also adds the {@link EntityIndex} of the
     * entity that the field belongs to.
     *
     * @return {@link FieldIndex}.
     */
    public FieldIndex addFieldIndex( String namespaceName, String entityName, String fieldName ) {
        addEntityIndex( namespaceName, entityName );

        String fullyQualifiedFieldName = namespaceName + "." + entityName + "." + fieldName;
        if ( !fieldIndexMapping.containsKey( fullyQualifiedFieldName ) ) {
            FieldIndex fieldIndex = FieldIndex.createIndex( namespaceName, entityName, fieldName );
            fieldIndexMapping.put( fieldIndex.fullyQualifiedName, fieldIndex );
        }

        return fieldIndexMapping.get( fullyQualifiedFieldName );
    }


    /**
     * Adds the first {@link EntityIndex} to {@link #queryRelation}.
     * It should only be called once, when adding the first {@link EntityIndex}.
     */
    public void addEntity( EntityIndex entityIndex ) {
        assert this.queryRelation == null;
        if ( log.isDebugEnabled() ) {
            log.debug( "Adding first EntityIndex '{}' for QueryRelation.", entityIndex.fullyQualifiedName );
        }
        this.queryRelation = new Tree<>( entityIndex );
        this.lastEntityIndex = entityIndex;
    }


    /**
     * Combines the existing {@link #queryRelation} with {@link EntityIndex}
     * using {@link Combiner}. It should only be called after {@link #addEntity(EntityIndex)}.
     *
     * @param entityIndex entity to be combined.
     * @param booleanGroup {@link BooleanGroup< EntityOpsBooleanOperator >} to
     * create {@link Combiner}.
     * @throws InvalidModifierException Thrown if invalid modifier names are used.
     */
    public void combineRelationWith( EntityIndex entityIndex, BooleanGroup<EntityOpsBooleanOperator> booleanGroup ) throws InvalidModifierException {
        assert this.queryRelation != null;

        if ( log.isDebugEnabled() ) {
            log.debug( "Creating combiner and combining QueryRelation with EntityIndex '{}'.", entityIndex.fullyQualifiedName );
        }

        Combiner combiner = Combiner.createCombiner( booleanGroup, this.lastEntityIndex, entityIndex );

        this.queryRelation = new Tree<>(
                this.queryRelation,
                combiner,
                new Tree<>( entityIndex )
        );

        this.lastEntityIndex = entityIndex;
    }


    /**
     * Creates a {@link Tree} leaf node using the input parameter ({@link Filter}).
     */
    public void addNewFilter( Filter filter ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Adding filter '{}'.", filter.toString() );
        }
        Tree<BooleanGroup<FieldOpsBooleanOperator>, Filter> root = new Tree<>( filter );
        this.filters.push( root );
    }


    /**
     * Merges the last two added {@link Filter}s using the {@link BooleanGroup< FieldOpsBooleanOperator >}
     */
    public void mergeFilterSubtreesWith( BooleanGroup<FieldOpsBooleanOperator> booleanGroup ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Merging filter subtrees with boolean group '{}'.", booleanGroup.toString() );
        }
        Tree<BooleanGroup<FieldOpsBooleanOperator>, Filter> right = this.filters.pop();
        Tree<BooleanGroup<FieldOpsBooleanOperator>, Filter> left = this.filters.pop();

        left = new Tree<>(
                left,
                booleanGroup,
                right
        );

        this.filters.push( left );
    }


    /**
     * Adds to the sort specification list.
     */
    public void addSortSpecification( FieldIndex fieldIndex, Map<String, Modifier> modifiers ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Adding sort specification for '{}'.", fieldIndex.fullyQualifiedName );
        }
        this.sortSpecifications.add(
                new Pair<>( fieldIndex, modifiers )
        );
    }


    /**
     * Creates and adds the {@link Projections.Projection}.
     */
    public void addProjection( FieldIndex fieldIndex, Map<String, Modifier> modifiers ) {
        projections.add( fieldIndex, modifiers );
    }


}
