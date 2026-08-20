/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo.relational;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.demo.DemoStore;
import org.polypheny.db.demo.Table;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.Result;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RelationalStore extends DemoStore {
    private final List<Table> tables;

    private final TransactionManager transactionManager;

    public RelationalStore( TransactionManager transactionManager, boolean local ) {
        super("demoposgresql", "sql", DataModel.RELATIONAL, "postgresql" );
        this.transactionManager = transactionManager;

        this.tables = new ArrayList<>();

        List<FieldInformation> albumFieldInformations = new ArrayList<>();
        albumFieldInformations.add(
                new FieldInformation(
                        "albumid",
                        new ColumnTypeInformation( PolyType.BIGINT, null, PolyType.BIGINT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, true),
                        Collation.CASE_INSENSITIVE,
                        PolyValue.getNull( PolyNumber.class),
                        0
                )
        );
        albumFieldInformations.add(
                new FieldInformation(
                        "title",
                        new ColumnTypeInformation( PolyType.TEXT, null, PolyType.TEXT.getMinPrecision(), PolyType.TEXT.getMinScale(), -1, -1, true),
                        Collation.CASE_INSENSITIVE,
                        PolyValue.getNull( PolyString.class ),
                        1
                )
        );
        albumFieldInformations.add(
                new FieldInformation(
                        "artistid",
                        new ColumnTypeInformation( PolyType.BIGINT, null, PolyType.BIGINT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, true),
                        Collation.CASE_INSENSITIVE,
                        PolyValue.getNull( PolyNumber.class ),
                        2
                )
        );
        this.tables.add( new Table( "album", albumFieldInformations, "/chinook/Album.json" ) );

        /*
        List<FieldInformation> artistFieldInformations = new ArrayList<>();
        artistFieldInformations.add( new FieldInformation( "ArtistId", new ColumnTypeInformation( PolyType.BIGINT, null, PolyType.BIGINT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, true), Collation.CASE_INSENSITIVE, null, 1 ) );
        artistFieldInformations.add( new FieldInformation( "Name", new ColumnTypeInformation( PolyType.TEXT, null, PolyType.TEXT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, true), Collation.CASE_INSENSITIVE, null, 1 ) );
        this.tables.add( new Table( "Album", artistFieldInformations, "/chinook/Artist.json" ) );
         */
    }


    @Override
    public void setupNamespace( Statement statement ) {
        try {
            java.sql.Statement sqlStatement = this.connection.createStatement();
            sqlStatement.execute( "CREATE TABLE album ( albumid BIGINT PRIMARY KEY, title TEXT, artistid BIGINT )" );
        }
        catch ( Exception e ) {
            log.error( e.getMessage() );
        }

        DdlManager ddlManager = DdlManager.getInstance();
        if (this.dataStore.isPresent()) {
            List<DataStore<?>> postgresql = List.of(this.dataStore.get());
            this.tables.forEach( table -> ddlManager.createTable( this.namespaceId, table.getName(), table.getColumns(), table.getConstraints(), true, postgresql, PlacementType.AUTOMATIC, statement ) );
        }
    }


    @Override
    public void loadData() {
        for (Table table: this.tables) {
            try {
                java.sql.PreparedStatement preparedStatement = this.connection.prepareStatement( "INSERT INTO album VALUES (?, ?, ?)" );
                List<Album> albums = this.loadJsonList( "/chinook/Album.json", Album.class );
                albums.forEach( album -> {
                    String query = String.format( "INSERT INTO Album (AlbumId, Title, ArtistId) VALUES (%s, '%s', %s);", album.albumId, album.title.replace( "'", "\\'" ), album.artistId );
                    try {
                        preparedStatement.setInt( 1, album.albumId );
                        preparedStatement.setString( 2, album.title );
                        preparedStatement.setInt( 3, album.albumId );
                        //preparedStatement.execute();
                    }
                    catch ( Exception e ) {
                        log.error( "{} FOR QUERY {}", e.getMessage(), query );
                    }
                });
            }
            catch ( Exception e ) {
                log.error( e.getMessage() );
            }
        }
    }

}
