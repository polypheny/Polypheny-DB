/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Store;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.util.CassandraTypesUtils;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
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
    public static final String DESCRIPTION = "An adapter for querying Cassandra.";
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
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( combinedTable.getSchema().name, combinedTable.getTable().name );
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
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( combinedTable.getSchema().name, combinedTable.getTable().name );
        SimpleStatement dropTable = SchemaBuilder.dropTable( this.dbKeyspace, physicalTableName ).build();

        // FIXME JS: Cassandra transaction hotfix
        context.getTransaction().registerInvolvedStore( this );
        this.session.execute( dropTable );
    }


    @Override
    public void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( catalogTable.getSchema().name, catalogTable.getTable().name );

        SimpleStatement addColumn = SchemaBuilder.alterTable( this.dbKeyspace, physicalTableName )
                .addColumn( catalogColumn.name, CassandraTypesUtils.getDataType( catalogColumn.type ) ).build();

        // FIXME JS: Cassandra transaction hotfix
        context.getTransaction().registerInvolvedStore( this );
        this.session.execute( addColumn );
    }


    @Override
    public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        CassandraPhysicalNameProvider physicalNameProvider = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() );
        String physicalTableName = physicalNameProvider.getPhysicalTableName( catalogTable.getSchema().name, catalogTable.getTable().name );

        SimpleStatement dropColumn = SchemaBuilder.alterTable( this.dbKeyspace, physicalTableName )
                .dropColumn( catalogColumn.name ).build();

        // FIXME JS: Cassandra transaction hotfix
        context.getTransaction().registerInvolvedStore( this );
        this.session.execute( dropColumn );
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
