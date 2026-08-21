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

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.demo.DemoStore;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.multimodel.PolyStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RelationalStore extends DemoStore {
    private final List<Table> tables;

    private final TransactionManager transactionManager;

    public RelationalStore( TransactionManager transactionManager, boolean local ) {
        super("demoposgresql", "sql", DataModel.RELATIONAL, "postgresql" );
        this.transactionManager = transactionManager;

        this.tables = createTables();

        /*
        List<FieldInformation> artistFieldInformations = new ArrayList<>();
        artistFieldInformations.add( new FieldInformation( "ArtistId", new ColumnTypeInformation( PolyType.BIGINT, null, PolyType.BIGINT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, true), Collation.CASE_INSENSITIVE, null, 1 ) );
        artistFieldInformations.add( new FieldInformation( "Name", new ColumnTypeInformation( PolyType.TEXT, null, PolyType.TEXT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, true), Collation.CASE_INSENSITIVE, null, 1 ) );
        this.tables.add( new Table( "Album", artistFieldInformations, "/chinook/Artist.json" ) );
         */
    }


    @Override
    public void setupNamespace( Statement statement ) {
        DdlManager ddlManager = DdlManager.getInstance();
        if (this.dataStore.isPresent()) {
            List<DataStore<?>> postgresql = List.of(this.dataStore.get());
            System.out.println(this.dataStore);
            this.tables.forEach( table -> {
                ddlManager.createTable( this.namespaceId, table.name(), table.columns(), table.constraints(), true, postgresql, PlacementType.AUTOMATIC, statement );
            } );
        }
    }


    @Override
    public void loadData() {
        for (Table table: this.tables) {
            try {
                String query = table.getPreparedStatementInsertQuery();
                java.sql.PreparedStatement preparedStatement = this.connection.prepareStatement( query );
                List<Album> albums = this.loadJsonList( "/chinook/Album.json", Album.class );
                albums.forEach( album -> {
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

    public List<Table> createTables() {
        List<Table> tables = new ArrayList<>();

        // Album Table

        List<FieldInformation> albumFieldInformations = new ArrayList<>();
        albumFieldInformations.add( this.getBigIntField( "albumid", true, 0 ) );
        albumFieldInformations.add( this.getStringField( "title", true, 1 ) );
        albumFieldInformations.add( this.getBigIntField( "artistid", true, 2 ) );

        List<ConstraintInformation> albumConstraintInformations = new ArrayList<>();
        albumConstraintInformations.add( new ConstraintInformation( "album_primary_key", ConstraintType.PRIMARY, List.of("albumid") ) );

        tables.add( new Table( "album", albumFieldInformations, albumConstraintInformations, "/chinook/Album.json" ) );

        // Genre Table

        List<FieldInformation> genreFieldInformation = new ArrayList<>();
        genreFieldInformation.add( this.getBigIntField( "genreid", true, 0 ) );
        genreFieldInformation.add( this.getStringField( "name", true, 1 ) );

        List<ConstraintInformation> genreConstraintInformations = new ArrayList<>();
        genreConstraintInformations.add( new ConstraintInformation( "genre_primary_key", ConstraintType.PRIMARY, List.of("genreid") ) );

        tables.add( new Table( "genre", genreFieldInformation, genreConstraintInformations, "/chinook/Genre.json" ) );

        // MediaType Table

        List<FieldInformation> mediaTypeFieldInformation = new ArrayList<>();
        mediaTypeFieldInformation.add( this.getBigIntField( "mediatypeid", true, 0 ) );
        mediaTypeFieldInformation.add( this.getStringField( "name", true, 1 ) );

        List<ConstraintInformation> mediatypeConstraintInformations = new ArrayList<>();
        mediatypeConstraintInformations.add( new ConstraintInformation( "mediatype_primary_key", ConstraintType.PRIMARY, List.of("mediatypeid") ) );

        tables.add( new Table( "mediatype", mediaTypeFieldInformation,  mediatypeConstraintInformations, "/chinook/MediaType.json" ) );

        // Artist Table

        List<FieldInformation> artistFieldInformation = new ArrayList<>();
        artistFieldInformation.add( this.getBigIntField( "artistid", true, 0 ) );
        artistFieldInformation.add( this.getStringField( "name", true, 0 ) );

        List<ConstraintInformation> artistConstraintInformations = new ArrayList<>();
        artistConstraintInformations.add( new ConstraintInformation( "artist_primary_key", ConstraintType.PRIMARY, List.of("artist") ) );

        tables.add( new Table( "artist", artistFieldInformation, artistConstraintInformations, "/chinook/Artist.json" ) );

        // Track Table

        List<FieldInformation> trackFieldInformation = new ArrayList<>();
        trackFieldInformation.add( this.getBigIntField( "trackid", true, 0 ) );
        trackFieldInformation.add( this.getStringField( "name", true, 1 ) );
        trackFieldInformation.add( this.getBigIntField( "albumid", true, 2 ) );
        trackFieldInformation.add( this.getBigIntField( "mediatypeid", true, 3 ) );
        trackFieldInformation.add( this.getBigIntField( "genreid", true, 4 ) );
        trackFieldInformation.add( this.getStringField( "composer", true, 5 ) );
        trackFieldInformation.add( this.getBigIntField( "milliseconds", true, 6 ) );
        trackFieldInformation.add( this.getBigIntField( "bytes", true, 7 ) );
        trackFieldInformation.add( this.getDecimalField( "unitprice", true, 8 ) );

        List<ConstraintInformation> trackConstraintInformations = new ArrayList<>();
        trackConstraintInformations.add( new ConstraintInformation( "artist_primary_key", ConstraintType.PRIMARY, List.of("artist") ) );

        tables.add( new Table( "track", trackFieldInformation, trackConstraintInformations, "/chinook/Track.json" ) );

        return tables;
    }

    public FieldInformation getBigIntField(String name, boolean nullable, int position) {
        return new FieldInformation(
                name,
                new ColumnTypeInformation( PolyType.BIGINT, null, PolyType.BIGINT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, false ),
                Collation.CASE_INSENSITIVE,
                PolyValue.fromType( 0, PolyType.BIGINT ),
                position
        );
    }

    public FieldInformation getDecimalField(String name, boolean nullable, int position) {
        return new FieldInformation(
                name,
                new ColumnTypeInformation( PolyType.DECIMAL, null, PolyType.DECIMAL.getMinPrecision(), PolyType.DECIMAL.getMinScale(), -1, -1, false ),
                Collation.CASE_INSENSITIVE,
                PolyValue.fromType( 0.0, PolyType.DECIMAL ),
                position
        );
    }

    public FieldInformation getStringField(String name, boolean nullable, int position) {
        return new FieldInformation(
                name,
                new ColumnTypeInformation( PolyType.VARCHAR, null, 255, PolyType.VARCHAR.getMinScale(), -1, -1, false ),
                Collation.CASE_INSENSITIVE,
                PolyValue.fromType( "", PolyType.VARCHAR ),
                position
        );
    }
}
