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
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
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


    private final Session session;
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
            Cluster cluster;
            List<InetSocketAddress> contactPoints = new ArrayList<>( 1 );
            contactPoints.add( new InetSocketAddress( this.dbHostname, this.dbPort ) );
            if ( this.dbUsername != null && this.dbPassword != null ) {
                cluster = Cluster.builder().addContactPointsWithPorts( contactPoints ).withCredentials( this.dbUsername, this.dbPassword ).build();
            } else {
                cluster = Cluster.builder().addContactPointsWithPorts( contactPoints ).build();
            }
            Session mySession;
            try {
                mySession = cluster.connect( this.dbKeyspace );
            } catch ( Exception e ) {
                Session tempSession = cluster.newSession();
                tempSession.execute( "CREATE KEYSPACE " + this.dbKeyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1}" );
                tempSession.close();
                mySession = cluster.connect( this.dbKeyspace );
            }
            this.session = mySession;
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name ) {
//        this.currentSchema = new CassandraSchema( this.dbHostname, this.dbPort, this.dbKeyspace, this.dbUsername, this.dbPassword, rootSchema, name );
        this.currentSchema = CassandraSchema.create( rootSchema, name, this.session, this.dbKeyspace );
    }


    @Override
    public Table createTableSchema( CatalogCombinedTable combinedTable ) {
        // TODO: Implement
//        log.warn( "createTableSchema is not implemented yet." );
        return new CassandraTable( this.currentSchema, combinedTable.getTable().name, false );
    }


    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogCombinedTable combinedTable ) {
        // TODO: Implement proper quoting
//        log.warn( "createTable is not implemented yet." );
        StringBuilder builder = new StringBuilder();
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( combinedTable.getSchema().name );
        qualifiedNames.add( combinedTable.getTable().name );
//        String physicalTableName = new CassandraPhysicalNameProvider( context.getTransaction().getCatalog() ).getPhyiscalTableName( qualifiedNames ).names.get( 0 );
        String physicalTableName = combinedTable.getTable().name;
        builder.append( "CREATE TABLE " ).append( physicalTableName ).append( " ( " );
        boolean first = true;
        for ( CatalogColumn column : combinedTable.getColumns() ) {
            if ( !first ) {
                builder.append( ", " );
            }
            builder.append( column.name ).append( " " );
            builder.append( getTypeString( column.type ) );
            if ( column.length != null ) {
                builder.append( "(" ).append( column.length );
                if ( column.scale != null ) {
                    builder.append( "," ).append( column.scale );
                }
                builder.append( ")" );
            }
            if ( first ) {
                builder.append( " PRIMARY KEY " );
            }
            first = false;
        }
        builder.append( ")" );
        executeUpdate( builder );
    }


    @Override
    public void dropTable( Context context, CatalogCombinedTable combinedTable ) {
        // TODO: Implement
        log.warn( "dropTable is not implemented yet." );
    }


    @Override
    public void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        // TODO: Implement
        log.warn( "addColumn is not implemented yet." );
    }


    @Override
    public void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn ) {
        // TODO: Implement
        log.warn( "dropColumn is not implemented yet." );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        // TODO: implement
        log.warn( "Not implemented yet" );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        // TODO: implement
        log.warn( "Not implemented yet" );
    }


    @Override
    public void truncate( Context context, CatalogCombinedTable table ) {
        // TODO: Implement
        log.warn( "truncate is not implemented yet." );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumn catalogColumn ) {
        // TODO: Implement
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
        // TODO: Implement
        try {
            this.session.close();
        } catch ( RuntimeException e ) {
            log.warn( "Exception while shutting down " + getUniqueName(), e );
        }
//        log.warn( "shutdown is not implemented yet." );
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement
        log.warn( "reloadSettings is not implemented yet." );
    }


    private String getTypeString( PolySqlType polySqlType ) {
        switch ( polySqlType ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                throw new RuntimeException( "Unsupported datatype: " + polySqlType.name() );
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case REAL:
                throw new RuntimeException( "Unsupported datatype: " + polySqlType.name() );
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case VARCHAR:
                return "VARCHAR";
            case TEXT:
                return "TEXT";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
        }
        throw new RuntimeException( "Unknown type: " + polySqlType.name() );
    }


    private void executeUpdate( StringBuilder builder ) {
        this.session.execute( builder.toString() );
    }

}
