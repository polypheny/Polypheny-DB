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
 */

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory;
import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public class CassandraStore extends DataStore {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "Cassandra";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "Apache Cassandra is an open-source wide-column store (i.e. a two-dimensional key–value store) designed to handle large amount of data. Cassandra can be deployed in a distributed manner.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Standalone", "Embedded" ) ),
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 9042 ),
            new AdapterSettingString( "keyspace", false, true, false, "cassandra" ),
            new AdapterSettingString( "username", false, true, false, "cassandra" ),
            new AdapterSettingString( "password", false, true, false, "" )
    );

    // Running embedded
    private final boolean isEmbedded;
    private final Cassandra embeddedCassandra;

    // Connection information
    private String dbHostname;
    private int dbPort;
    private String dbKeyspace;
    private String dbUsername;
    private String dbPassword;

    // Array Container UDT
    private final UserDefinedType arrayContainerUdt;

    // Only display specific logging messages once
    private static boolean displayedPrepareLoggingMessage = false;
    private static boolean displayedCommitLoggingMessage = false;

    private final CqlSession session;
    private CassandraSchema currentSchema;


    public CassandraStore( int storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );

        // Parse settings
        this.dbHostname = settings.get( "host" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );
        this.dbKeyspace = settings.get( "keyspace" );
        this.dbUsername = settings.get( "username" );
        this.dbPassword = settings.get( "password" );
        this.isEmbedded = settings.get( "type" ).equalsIgnoreCase( "Embedded" );

        if ( this.isEmbedded ) {
            // Making sure we are on java 8, as cassandra does not support anything newer!
            // This is a cassandra issue. It is also marked as "won't fix"...
            // See: https://issues.apache.org/jira/browse/CASSANDRA-13107

            if ( !System.getProperty( "java.version" ).startsWith( "1.8" ) ) {
                log.error( "Embedded cassandra requires Java 8 to work. Currently using: {}. Aborting!", System.getProperty( "java.version" ) );
                throw new RuntimeException( "Embedded cassandra requires Java 8 to be used!" );
            }

            // Setting up the embedded instance of cassandra.
            log.debug( "Attempting to create embedded cassandra instance." );
            EmbeddedCassandraFactory cassandraFactory = new EmbeddedCassandraFactory();
//            cassandraFactory.setJavaHome( Paths.get( System.getenv( "JAVA_HOME" ) ) );
            this.embeddedCassandra = cassandraFactory.create();
            this.embeddedCassandra.start();

            this.dbHostname = this.embeddedCassandra.getAddress().getHostAddress();
            this.dbPort = this.embeddedCassandra.getPort();

            log.warn( "Embedded cassandra address: {}:{}", this.dbHostname, this.dbPort );
        } else {
            this.embeddedCassandra = null;
        }

        try {
            CqlSessionBuilder cluster = CqlSession.builder();
            cluster.withLocalDatacenter( "datacenter1" );
            List<InetSocketAddress> contactPoints = new ArrayList<>( 1 );
            contactPoints.add( new InetSocketAddress( this.dbHostname, this.dbPort ) );
            if ( this.dbUsername != null && this.dbPassword != null ) {
                cluster.addContactPoints( contactPoints ).withAuthCredentials( this.dbUsername, this.dbPassword );
            } else {
                cluster.addContactPoints( contactPoints );
            }
            CqlSession mySession;
            mySession = cluster.build();
            try {
                CreateKeyspace createKs = SchemaBuilder.createKeyspace( this.dbKeyspace ).ifNotExists().withSimpleStrategy( 1 );
                mySession.execute( createKs.build() );
                mySession.execute( "USE " + this.dbKeyspace );
            } catch ( Exception e ) {
                log.warn( "Unable to use keyspace {}.", this.dbKeyspace, e );
                mySession.execute( "CREATE KEYSPACE " + this.dbKeyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1}" );
                mySession.execute( "USE KEYSPACE " + this.dbKeyspace );
            }

            mySession.execute( "CREATE TYPE IF NOT EXISTS " + this.dbKeyspace + ".arraycontainer ( innertype text, dimension int, cardinality int, data text );" );
            arrayContainerUdt =
                    mySession.getMetadata()
                            .getKeyspace( this.dbKeyspace )
                            .flatMap( ks -> ks.getUserDefinedType( "arraycontainer" ) )
                            .orElseThrow( () -> new IllegalArgumentException( "Missing UDT definition" ) );

            this.session = mySession;
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        this.currentSchema = CassandraSchema.create(
                rootSchema,
                name,
                this.session,
                this.dbKeyspace,
                new CassandraPhysicalNameProvider( this.getAdapterId() ),
                this,
                this.arrayContainerUdt );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        String physicalTableName = currentSchema.getConvention().physicalNameProvider.getPhysicalTableName( catalogTable.id );
        return new CassandraTable( this.currentSchema, catalogTable.name, physicalTableName, false );
    }


    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        // This check is probably not required due to the check below it.
        if ( catalogTable.primaryKey == null ) {
            throw new UnsupportedOperationException( "Cannot create Cassandra Table without a primary key!" );
        }

        long primaryKeyColumn = -1;
        List<Long> keyColumns = new ArrayList<>();

        for ( CatalogKey catalogKey : catalog.getTableKeys( catalogTable.id ) ) {
            keyColumns.addAll( catalogKey.columnIds );
            // TODO JS: make sure there's only one primary key!
            if ( primaryKeyColumn == -1 ) {
                primaryKeyColumn = catalogKey.columnIds.get( 0 );
            }
        }

        if ( primaryKeyColumn == -1 ) {
            throw new UnsupportedOperationException( "Cannot create Cassandra Table without a primary key!" );
        }

        final long primaryKeyColumnLambda = primaryKeyColumn;

        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( this.getAdapterId() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( catalogTable.id );
        // List<CatalogColumn> columns = combinedTable.getColumns();
        List<CatalogColumnPlacement> columns = catalog.getColumnPlacementsOnAdapter( getAdapterId(), catalogTable.id );
        CatalogColumnPlacement primaryColumnPlacement = columns.stream().filter( c -> c.columnId == primaryKeyColumnLambda ).findFirst().get();
        CatalogColumn catalogColumn = catalog.getColumn( primaryColumnPlacement.columnId );

        CreateTable createTable = SchemaBuilder.createTable( this.dbKeyspace, physicalTableName )
                .withPartitionKey( physicalNameProvider.generatePhysicalColumnName( catalogColumn.id ), CassandraTypesUtils.getDataType( catalogColumn.type, this.arrayContainerUdt ) );

        for ( CatalogColumnPlacement placement : columns ) {
            catalogColumn = catalog.getColumn( placement.columnId );
            DataType fieldType;
            if ( catalogColumn.collectionsType == PolyType.ARRAY ) {
                fieldType = this.arrayContainerUdt;
            } else {
                fieldType = CassandraTypesUtils.getDataType( catalogColumn.type, null );
            }

            if ( keyColumns.contains( placement.columnId ) ) {
                if ( placement.columnId != primaryKeyColumn ) {
                    createTable = createTable.withClusteringColumn( physicalNameProvider.generatePhysicalColumnName( placement.columnId ), CassandraTypesUtils.getDataType( catalogColumn.type, this.arrayContainerUdt ) );
                }
            } else {
                createTable = createTable.withColumn( physicalNameProvider.generatePhysicalColumnName( placement.columnId ), fieldType );
            }
        }

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        this.session.execute( createTable.build() );

        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapter( getAdapterId(), catalogTable.id ) ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    placement.columnId,
                    this.dbKeyspace, // TODO MV: physical schema name
                    physicalTableName,
                    physicalNameProvider.generatePhysicalColumnName( placement.columnId ),
                    true );
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable ) {
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( this.getAdapterId() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( catalogTable.id );
        SimpleStatement dropTable = SchemaBuilder.dropTable( this.dbKeyspace, physicalTableName ).build();

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        this.session.execute( dropTable );
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( this.getAdapterId() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( catalogTable.id );
        String physicalColumnName = physicalNameProvider.generatePhysicalColumnName( catalogColumn.id );

        SimpleStatement addColumn = SchemaBuilder.alterTable( this.dbKeyspace, physicalTableName )
                .addColumn( physicalColumnName, CassandraTypesUtils.getDataType( catalogColumn.type, this.arrayContainerUdt ) ).build();

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        // TODO JS: Wrap with error handling to check whether successful, if not, try iterative revision names to find one that works.
        this.session.execute( addColumn );

        catalog.updateColumnPlacementPhysicalNames(
                getAdapterId(),
                catalogColumn.id,
                this.dbKeyspace,
                physicalTableName,
                physicalColumnName,
                false );
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
//        public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
//        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getStatement().getTransaction().getCatalog(), this.getStoreId() );
        String physicalTableName = columnPlacement.physicalTableName;
        String physicalColumnName = columnPlacement.physicalColumnName;

        SimpleStatement dropColumn = SchemaBuilder.alterTable( this.dbKeyspace, physicalTableName )
                .dropColumn( physicalColumnName ).build();

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        this.session.execute( dropColumn );
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex ) {
        throw new RuntimeException( "Cassandra adapter does not support adding indexes" );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex ) {
        throw new RuntimeException( "Cassandra adaper does not support dropping indexes" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        // TODO JS: implement cassandra prepare
        if ( !displayedPrepareLoggingMessage ) {
            log.warn( "Prepare is not yet supported. This warning will not be repeated!" );
            displayedPrepareLoggingMessage = true;
        }
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        // TODO JS: implement cassandra commit
        if ( !displayedCommitLoggingMessage ) {
            log.warn( "Commit is not yet supported. This warning will not be repeated!" );
            displayedCommitLoggingMessage = true;
        }
    }


    @Override
    public void rollback( PolyXid xid ) {
        // TODO JS: implement cassandra rollback
        log.warn( "Rollback is not yet supported." );
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( this.getAdapterId() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( table.id );
        SimpleStatement truncateTable = QueryBuilder.truncate( this.dbKeyspace, physicalTableName ).build();

        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        this.session.execute( truncateTable );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement placement, CatalogColumn catalogColumn ) {
//    public void updateColumnType( Context context, CatalogColumn catalogColumn ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );

        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( this.getAdapterId() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( catalogColumn.tableId );

//        SimpleStatement selectData = QueryBuilder.selectFrom( this.dbKeyspace, physicalTableName ).all().build();
        SimpleStatement selectData = QueryBuilder.selectFrom( this.dbKeyspace, physicalTableName ).all().build();
        ResultSet rs = this.session.execute( selectData );

        if ( !rs.isFullyFetched() ) {
            throw new RuntimeException( "Unable to convert column type..." );
        }

        String physicalColumnName = physicalNameProvider.getPhysicalColumnName( catalogColumn.id );

        String newPhysicalColumnName = CassandraPhysicalNameProvider.incrementNameRevision( physicalColumnName );

        BatchStatementBuilder builder = new BatchStatementBuilder( BatchType.LOGGED );
        RelationMetadata relationMetadata = session.getMetadata().getKeyspace( dbKeyspace ).get().getTable( physicalTableName ).get();
        List<ColumnMetadata> primaryKeys = relationMetadata.getPrimaryKey();
        ColumnMetadata oldColumn = relationMetadata.getColumn( physicalColumnName ).get();
        PolyType oldType = CassandraTypesUtils.getPolyType( oldColumn.getType() );

//        PolyTypeAssignmentRules rules = PolyTypeAssignmentRules.instance( true );
//        if ( ! rules.canCastFrom( catalogColumn.type, oldType )) {
//            throw new RuntimeException( "Unable to change column type. Unable to cast " + oldType.getName() + " to " + catalogColumn.type.getName() + "." );
//        }

        Function<Object, Object> converter = CassandraTypesUtils.convertToFrom( catalogColumn.type, oldType );

        session.execute( SchemaBuilder.alterTable( this.dbKeyspace, physicalTableName )
                .addColumn( newPhysicalColumnName, CassandraTypesUtils.getDataType( catalogColumn.type, this.arrayContainerUdt ) )
                .build() );

        for ( Row r : rs ) {
            List<Relation> whereClause = new ArrayList<>();
            for ( ColumnMetadata cm : primaryKeys ) {
                Relation rl = Relation.column( cm.getName() ).isEqualTo(
                        QueryBuilder.literal( r.get( cm.getName(), CassandraTypesUtils.getJavaType( cm.getType() ) ) )
//                        QueryBuilder.literal( r.get( cm.getName(), CassandraTypesUtils.getPolyType( cm.getType() ).getTypeJavaClass() ) )
                );
                whereClause.add( rl );
            }

            Object oldValue = r.get( physicalColumnName, CassandraTypesUtils.getJavaType( oldColumn.getType() ) );
//            Object oldValue = r.get( physicalColumnName, oldType.getTypeJavaClass() );

            builder.addStatement(
                    QueryBuilder.update( this.dbKeyspace, physicalTableName )
                            .set( Assignment.setColumn(
                                    newPhysicalColumnName,
                                    QueryBuilder.literal( converter.apply( oldValue ) ) ) )
                            .where( whereClause )
                            .build()
            );
        }

        this.session.execute( builder.build() );

        session.execute( SchemaBuilder.alterTable( this.dbKeyspace, physicalTableName )
                .dropColumn( physicalColumnName ).build() );

        physicalNameProvider.updatePhysicalColumnName( catalogColumn.id, newPhysicalColumnName, true );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return new ArrayList<>();
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        throw new RuntimeException( "Cassandra adapter does not support adding indexes" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        List<Long> pkIds = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey ).columnIds;
        return ImmutableList.of( new FunctionalIndexInfo( pkIds, "PRIMARY (unique)" ) );
    }


    @Override
    public void shutdown() {
        try {
            this.session.close();
        } catch ( RuntimeException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }

        if ( this.isEmbedded ) {
            this.embeddedCassandra.stop();
        }

        log.info( "Shut down Cassandra store: {}", this.getUniqueName() );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO JS: Implement
        log.warn( "reloadSettings is not implemented yet." );
    }

}
