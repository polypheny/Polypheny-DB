/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Schema mapped onto a Cassandra column family
 */
@Slf4j
public class CassandraSchema extends AbstractSchema {

    private final static Pattern columnIdPattern = Pattern.compile( "^col([0-9]+)(r([0-9]+))?" );

    @Getter
    final CqlSession session;
    final String keyspace;
    private final SchemaPlus parentSchema;
    final String name;

    private final UserDefinedType arrayContainerUdt;


    @Getter
    private final CassandraConvention convention;

    private final CassandraStore cassandraStore;

    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    private static final int DEFAULT_CASSANDRA_PORT = 9042;


    private CassandraSchema( CqlSession session, String keyspace, SchemaPlus parentSchema, String name, CassandraConvention convention, CassandraStore cassandraStore, UserDefinedType arrayContainerUdt ) {
        super();
        this.session = session;
        this.keyspace = keyspace;
        this.parentSchema = parentSchema;
        this.name = name;
        this.convention = convention;
        this.cassandraStore = cassandraStore;
        this.arrayContainerUdt = arrayContainerUdt;
    }


    public static CassandraSchema create(
            SchemaPlus parentSchema,
            String name,
            CqlSession session,
            String keyspace,
            CassandraPhysicalNameProvider physicalNameProvider,
            CassandraStore cassandraStore,
            UserDefinedType arrayContainerUdt ) {
        final Expression expression = Schemas.subSchemaExpression( parentSchema, name, CassandraSchema.class );
        final CassandraConvention convention = new CassandraConvention( name, expression, physicalNameProvider, arrayContainerUdt );
        return new CassandraSchema( session, keyspace, parentSchema, name, convention, cassandraStore, arrayContainerUdt );
    }


    public void registerStore( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( this.cassandraStore );
    }


    private String logicalColumnFromPhysical( String physicalColumnName ) {
        Matcher m = columnIdPattern.matcher( physicalColumnName );
        Long columnId;
        if ( m.find() ) {
            columnId = Long.valueOf( m.group( 1 ) );
        } else {
            throw new RuntimeException( "Unable to find column id in physical column name: " + physicalColumnName );
        }

        return convention.physicalNameProvider.getLogicalColumnName( columnId );
    }


    private CatalogColumn logicalColumnFromPhysicalColumn( String physicalColumnName ) {
        Matcher m = columnIdPattern.matcher( physicalColumnName );
        Long columnId;
        if ( m.find() ) {
            columnId = Long.valueOf( m.group( 1 ) );
        } else {
            throw new RuntimeException( "Unable to find column id in physical column name: " + physicalColumnName );
        }

        return convention.physicalNameProvider.getLogicalColumn( columnId );
    }


    AlgProtoDataType getAlgDataType( String physicalTableName, boolean view ) {
        Map<CqlIdentifier, ColumnMetadata> columns;
        if ( view ) {
            throw new RuntimeException( "Views are currently broken." );
        } else {
            columns = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get().getColumns();
        }

        // Temporary type factory, just for the duration of this method. Allowable because we're creating a proto-type, not a type; before being used, the proto-type will be copied into a real type factory.
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
//        Pattern columnIdPattern = Pattern.compile( "^col([0-9]+)(r([0-9]+))?" );

//        List<Pair<Integer, Entry<CqlIdentifier, ColumnMetadata>>> preorderedList = new ArrayList<>();
        List<Pair<Integer, RowTypeGeneratorContainer>> preorderedList = new ArrayList<>();

        for ( Entry<CqlIdentifier, ColumnMetadata> column : columns.entrySet() ) {
            final String physicalColumnName = column.getKey().toString();
            final DataType type = column.getValue().getType();

            // TODO: This mapping of types can be done much better
            PolyType typeName = CassandraTypesUtils.getPolyType( type );

            // TODO (PCP)
            /*Matcher m = columnIdPattern.matcher( physicalColumnName );
            Long columnId;
            if ( m.find() ) {
                columnId = Long.valueOf( m.group( 1 ) );
            } else {
                throw new RuntimeException( "Unable to find column id in physical column name: " + physicalColumnName );
            }
            String logicalColumnName = convention.physicalNameProvider.getLogicalColumnName( columnId );*/
            CatalogColumn logicalColumn = this.logicalColumnFromPhysicalColumn( physicalColumnName );
            String logicalColumnName = this.logicalColumnFromPhysical( physicalColumnName );

            AlgDataType algDataType;
            if ( logicalColumn.collectionsType == PolyType.ARRAY ) {
                AlgDataType innerType = typeFactory.createPolyType( logicalColumn.type );
                algDataType = typeFactory.createArrayType( innerType, logicalColumn.cardinality, logicalColumn.dimension );
            } else {
                algDataType = typeFactory.createPolyType( logicalColumn.type );
            }

            preorderedList.add( new Pair<>( logicalColumn.position, new RowTypeGeneratorContainer( logicalColumnName, physicalColumnName, algDataType ) ) );
//            fieldInfo.add( logicalColumnName, physicalColumnName, typeFactory.createPolyType( typeName ) ).nullable( true );
        }

        preorderedList.sort( Comparator.naturalOrder() );

        for ( Pair<Integer, RowTypeGeneratorContainer> containerPair : preorderedList ) {
            RowTypeGeneratorContainer container = containerPair.right;
            fieldInfo.add( container.logicalName, container.physicalName, container.dataType ).nullable( true );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    /**
     * Get all primary key columns from the underlying CQL table
     *
     * @return A list of field names that are part of the partition and clustering keys
     */
    Pair<List<String>, List<String>> getKeyFields( String physicalTableName, boolean view ) {
        RelationMetadata relation;
//        List<String> qualifiedNames = new LinkedList<>();
//        qualifiedNames.add( this.name );
//        qualifiedNames.add( columnFamily );
//        String physicalTableName = this.convention.physicalNameProvider.getPhysicalTableName( qualifiedNames );
        if ( view ) {
            relation = getKeyspace().getView( "\"" + physicalTableName + "\"" ).get();
        } else {
            relation = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get();
        }

        List<ColumnMetadata> partitionKey = relation.getPartitionKey();
        List<String> pKeyFields = new ArrayList<>();
        for ( ColumnMetadata column : partitionKey ) {
            pKeyFields.add( this.logicalColumnFromPhysical( column.getName().toString() ) );
//            pKeyFields.add(  column.getName().toString() );
        }

        Map<ColumnMetadata, ClusteringOrder> clusteringKey = relation.getClusteringColumns();
        List<String> cKeyFields = new ArrayList<>();
        for ( Entry<ColumnMetadata, ClusteringOrder> column : clusteringKey.entrySet() ) {
            cKeyFields.add( this.logicalColumnFromPhysical( column.getKey().getName().asInternal() ) );
//            cKeyFields.add( column.getKey().toString() );
        }

        return Pair.of( ImmutableList.copyOf( pKeyFields ), ImmutableList.copyOf( cKeyFields ) );
    }


    /**
     * Get all primary key columns from the underlying CQL table
     *
     * @return A list of field names that are part of the partition and clustering keys
     */
    Pair<List<String>, List<String>> getPhysicalKeyFields( String physicalTableName, boolean view ) {
        RelationMetadata relation;
//        List<String> qualifiedNames = new LinkedList<>();
//        qualifiedNames.add( this.name );
//        qualifiedNames.add( columnFamily );
//        String physicalTableName = this.convention.physicalNameProvider.getPhysicalTableName( qualifiedNames );
        if ( view ) {
            relation = getKeyspace().getView( "\"" + physicalTableName + "\"" ).get();
        } else {
            relation = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get();
        }

        List<ColumnMetadata> partitionKey = relation.getPartitionKey();
        List<String> pKeyFields = new ArrayList<>();
        for ( ColumnMetadata column : partitionKey ) {
//            pKeyFields.add( this.logicalColumnFromPhysical( column.getName().toString() ) );
            pKeyFields.add( column.getName().toString() );
        }

        Map<ColumnMetadata, ClusteringOrder> clusteringKey = relation.getClusteringColumns();
        List<String> cKeyFields = new ArrayList<>();
        for ( Entry<ColumnMetadata, ClusteringOrder> column : clusteringKey.entrySet() ) {
//            cKeyFields.add( this.logicalColumnFromPhysical( column.getKey().toString() ) );
            cKeyFields.add( column.getKey().toString() );
        }

        return Pair.of( ImmutableList.copyOf( pKeyFields ), ImmutableList.copyOf( cKeyFields ) );
    }


    /**
     * Get the collation of all clustering key columns.
     *
     * @return A RelCollations representing the collation of all clustering keys
     */
    public List<AlgFieldCollation> getClusteringOrder( String physicalTableName, boolean view ) {
        RelationMetadata relation;
//        List<String> qualifiedNames = new LinkedList<>();
//        qualifiedNames.add( this.name );
//        qualifiedNames.add( columnFamily );
//        String physicalTableName = this.convention.physicalNameProvider.getPhysicalTableName( qualifiedNames );
        if ( view ) {
//            throw new RuntimeException( "Views are currently broken." );
            relation = getKeyspace().getView( "\"" + physicalTableName + "\"" ).get();
        } else {
            relation = getKeyspace().getTable( "\"" + physicalTableName + "\"" ).get();
        }

        Map<ColumnMetadata, ClusteringOrder> clusteringOrder = relation.getClusteringColumns();
        List<AlgFieldCollation> keyCollations = new ArrayList<>();

        int i = 0;
        for ( Entry<ColumnMetadata, ClusteringOrder> order : clusteringOrder.entrySet() ) {
            AlgFieldCollation.Direction direction;
            switch ( order.getValue() ) {
                case DESC:
                    direction = AlgFieldCollation.Direction.DESCENDING;
                    break;
                case ASC:
                default:
                    direction = AlgFieldCollation.Direction.ASCENDING;
                    break;
            }
            CatalogColumn logicalColumn = this.logicalColumnFromPhysicalColumn( order.getKey().getName().asInternal() );
            keyCollations.add( new AlgFieldCollation( logicalColumn.position - 1, direction ) );
            i++;
        }

        return keyCollations;
    }


    // FIXME JS: Do not regenerate TableMap every time we call this!
    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for ( Entry<CqlIdentifier, TableMetadata> table : getKeyspace().getTables().entrySet() ) {
            builder.put( table.getKey().toString(), new CassandraTable( this, table.getKey().toString() ) );

            // TODO JS: Fix the view situation!
            /*for ( MaterializedViewMetadata view : table.getValue().getViews() ) {
                String viewName = view.getName();
                builder.put( viewName, new CassandraTable( this, viewName, true ) );
            }*/
        }
        return builder.build();
    }


    private KeyspaceMetadata getKeyspace() {
        Optional<KeyspaceMetadata> metadata = session.getMetadata().getKeyspace( keyspace );
        if ( metadata.isPresent() ) {
            return metadata.get();
        } else {
            throw new RuntimeException( "There is no metadata." );
        }
    }


    @AllArgsConstructor
    private class RowTypeGeneratorContainer {

        String logicalName;
        String physicalName;
        AlgDataType dataType;

    }

}

