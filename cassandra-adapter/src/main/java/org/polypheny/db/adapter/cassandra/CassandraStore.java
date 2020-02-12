/*
 * Copyright 2019-2020 The Polypheny Project
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


import org.polypheny.db.PolyXid;
import org.polypheny.db.Store;
import org.polypheny.db.Transaction;
import org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CassandraStore extends Store {

    @SuppressWarnings("WeakerAccess")
    public static final String ADAPTER_NAME = "Cassandra";
    @SuppressWarnings("WeakerAccess")
    public static final String DESCRIPTION = "Apache Cassandra is an open-source key-value store designed to handle large amount of data. Cassandra can be deployed in a distributed manner.";
    @SuppressWarnings("WeakerAccess")
    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 9042 ),
            new AdapterSettingString( "keyspace", false, true, false, "cassandra" ),
            new AdapterSettingString( "username", false, true, false, "cassandra" ),
            new AdapterSettingString( "password", false, true, false, "" )
    );

    // Connection information
    private String dbHostname;
    private int dbPort;
    private String dbKeyspace;
    private String dbUsername;
    private String dbPassword;


    private final CqlSession session;
    private CassandraSchema currentSchema;


    public CassandraStore( int storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings );

        // Parse settings
        this.dbHostname = settings.get( "host" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );
        this.dbKeyspace = settings.get( "keyspace" );
        this.dbUsername = settings.get( "username" );
        this.dbPassword = settings.get( "password" );

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
            this.session = mySession;
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name ) {
        this.currentSchema = CassandraSchema.create( rootSchema, name, this.session, this.dbKeyspace, new CassandraPhysicalNameProvider( transaction.getCatalog() ), this );
    }


    @Override
    public Table createTableSchema( CatalogCombinedTable combinedTable ) {
        return new CassandraTable( this.currentSchema, combinedTable.getTable().name, false );
    }


    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogCombinedTable combinedTable ) {
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( qualifiedNames );
        List<CatalogColumn> columns = combinedTable.getColumns();
        CatalogColumn column = columns.remove( 0 );
        CreateTable createTable = SchemaBuilder.createTable( this.dbKeyspace, physicalTableName )
                .withPartitionKey( column.name, CassandraTypesUtils.getDataType( column.type ) );

        for ( CatalogColumn c : columns ) {
            createTable = createTable.withColumn( c.name, CassandraTypesUtils.getDataType( c.type ) );
        }

        // FIXME JS: Cassandra transaction hotfix
        context.getTransaction().registerInvolvedStore( this );
        this.session.execute( createTable.build() );
    }


    @Override
    public void dropTable( Context context, CatalogCombinedTable combinedTable ) {
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( qualifiedNames );
        SimpleStatement dropTable = SchemaBuilder.dropTable( this.dbKeyspace, physicalTableName ).build();

        // FIXME JS: Cassandra transaction hotfix
        context.getTransaction().registerInvolvedStore( this );
        this.session.execute( dropTable );
    }


    @Override
    public void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        // TODO JS: Implement
        log.warn( "addColumn is not implemented yet." );
    }


    @Override
    public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        // TODO JS: Implement
        log.warn( "dropColumn is not implemented yet." );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        // TODO JS: implement cassandra prepare
        log.warn( "Prepare is not yet supported." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        // TODO JS: implement cassandra commit
        log.warn( "Commit is not yet supported." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        // TODO JS: implement cassandra rollback
        log.warn( "Rollback is not yet supported." );
    }


    @Override
    public void truncate( Context context, CatalogCombinedTable table ) {
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( table.getSchema().name );
        qualifiedNames.add( table.getTable().name );
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( qualifiedNames );
        SimpleStatement truncateTable = QueryBuilder.truncate( this.dbKeyspace, physicalTableName ).build();

        // FIXME JS: Cassandra transaction hotfix
        context.getTransaction().registerInvolvedStore( this );
        this.session.execute( truncateTable );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumn catalogColumn ) {
        // TODO JS: Implement
        log.warn( "updateColumnType is not implemented yet." );
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
    public void shutdown() {
        try {
            this.session.close();
        } catch ( RuntimeException e ) {
            log.warn( "Exception while shutting down " + getUniqueName(), e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO JS: Implement
        log.warn( "reloadSettings is not implemented yet." );
    }

}
