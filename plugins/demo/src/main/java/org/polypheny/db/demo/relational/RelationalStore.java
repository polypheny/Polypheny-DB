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
import org.polypheny.db.type.entity.PolyValue;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RelationalStore extends DemoStore {
    private final List<Table<? extends IPreperable>> tables;

    private final TransactionManager transactionManager;

    public RelationalStore( TransactionManager transactionManager, boolean local ) {
        super("demoposgresql", "sql", DataModel.RELATIONAL, "postgresql" );
        this.transactionManager = transactionManager;
        this.tables = createTables();
    }


    @Override
    public void setupNamespace( Statement statement ) {
        DdlManager ddlManager = DdlManager.getInstance();
        if (this.dataStore.isPresent()) {
            List<DataStore<?>> postgresql = List.of( this.dataStore.get() );
            this.tables.forEach( table -> {
                ddlManager.createTable( this.namespaceId, table.name(), table.columns(), table.constraints(), true, postgresql, PlacementType.AUTOMATIC, statement );
            } );
        }
    }


    @Override
    public void loadData() {
        for (Table<? extends IPreperable> table: this.tables) {
            try {
                String query = table.getPreparedStatementInsertQuery();
                java.sql.PreparedStatement preparedStatement = this.connection.prepareStatement( query );
                List<? extends IPreperable> data = this.loadJsonList( table.file(), table.type() );
                data.forEach( value -> {
                    try {
                        if ( !value.filter() ) {
                            value.setValues( preparedStatement );
                            preparedStatement.addBatch();
                        }
                    }
                    catch ( Exception e ) {
                        log.error( "Exception while adding SQL batch for query: {}. {}", query, e.getMessage() );
                    }
                });

                preparedStatement.executeBatch();
            }
            catch ( Exception e ) {
                log.error( "Exception while loading relational data. {}", e.getMessage() );
            }
        }
    }

    public List<Table<? extends IPreperable>> createTables() {
        // Album Table
        List<FieldInformation> albumFieldInformations = new ArrayList<>();
        albumFieldInformations.add( this.getBigIntField( "albumid", 1 ) );
        albumFieldInformations.add( this.getStringField( "title", 2 ) );
        albumFieldInformations.add( this.getBigIntField( "artistid", 3 ) );

        List<ConstraintInformation> albumConstraintInformations = new ArrayList<>();
        albumConstraintInformations.add( new ConstraintInformation( "album_primary_key", ConstraintType.PRIMARY, List.of("albumid") ) );
        albumConstraintInformations.add( new ConstraintInformation( "artist_foreign_key", ConstraintType.FOREIGN, List.of("artistid"), "artist", "artistid" ) );

        Table<Album> albumTable = new Table<>( "album", albumFieldInformations, albumConstraintInformations, "/relational/Album.json", Album.class );

        // Genre Table

        List<FieldInformation> genreFieldInformation = new ArrayList<>();
        genreFieldInformation.add( this.getBigIntField( "genreid", 1 ) );
        genreFieldInformation.add( this.getStringField( "name", 2 ) );

        List<ConstraintInformation> genreConstraintInformations = new ArrayList<>();
        genreConstraintInformations.add( new ConstraintInformation( "genre_primary_key", ConstraintType.PRIMARY, List.of("genreid") ) );

        Table<Genre> genreTable = new Table<>( "genre", genreFieldInformation, genreConstraintInformations, "/relational/Genre.json", Genre.class );

        // MediaType Table

        List<FieldInformation> mediaTypeFieldInformation = new ArrayList<>();
        mediaTypeFieldInformation.add( this.getBigIntField( "mediatypeid", 1 ) );
        mediaTypeFieldInformation.add( this.getStringField( "name", 2 ) );

        List<ConstraintInformation> mediatypeConstraintInformations = new ArrayList<>();
        mediatypeConstraintInformations.add( new ConstraintInformation( "mediatype_primary_key", ConstraintType.PRIMARY, List.of("mediatypeid") ) );

        Table<MediaType> mediaTypeTable = new Table<>( "mediatype", mediaTypeFieldInformation,  mediatypeConstraintInformations, "/relational/MediaType.json", MediaType.class );

        // Artist Table

        List<FieldInformation> artistFieldInformation = new ArrayList<>();
        artistFieldInformation.add( this.getBigIntField( "artistid", 1 ) );
        artistFieldInformation.add( this.getStringField( "name", 2 ) );

        List<ConstraintInformation> artistConstraintInformations = new ArrayList<>();
        artistConstraintInformations.add( new ConstraintInformation( "artist_primary_key", ConstraintType.PRIMARY, List.of("artistid") ) );

        Table<Artist> artistTable = new Table<>( "artist", artistFieldInformation, artistConstraintInformations, "/relational/Artist.json", Artist.class );

        // Track Table

        List<FieldInformation> trackFieldInformation = new ArrayList<>();
        trackFieldInformation.add( this.getBigIntField( "trackid", 1 ) );
        trackFieldInformation.add( this.getStringField( "name", 2 ) );
        trackFieldInformation.add( this.getBigIntField( "albumid", 3 ) );
        trackFieldInformation.add( this.getBigIntField( "mediatypeid", 4 ) );
        trackFieldInformation.add( this.getBigIntField( "genreid", 5 ) );
        trackFieldInformation.add( this.getStringField( "composer", 6 ) );
        trackFieldInformation.add( this.getBigIntField( "milliseconds", 7 ) );
        trackFieldInformation.add( this.getBigIntField( "bytes", 8 ) );
        trackFieldInformation.add( this.getDecimalField( "unitprice", 9 ) );

        List<ConstraintInformation> trackConstraintInformations = new ArrayList<>();
        trackConstraintInformations.add( new ConstraintInformation( "track_primary_key", ConstraintType.PRIMARY, List.of("trackid") ) );
        trackConstraintInformations.add( new ConstraintInformation( "album_foreign_key", ConstraintType.FOREIGN, List.of("albumid"), "album", "albumid" ) );
        trackConstraintInformations.add( new ConstraintInformation( "mediatype_foreign_key", ConstraintType.FOREIGN, List.of("mediatypeid"), "mediatype", "mediatypeid" ) );
        trackConstraintInformations.add( new ConstraintInformation( "genre_foreign_key", ConstraintType.FOREIGN, List.of("genreid"), "genre", "genreid" ) );

        Table<Track> trackTable = new Table<>( "track", trackFieldInformation, trackConstraintInformations, "/relational/Track.json", Track.class );

        return List.of(mediaTypeTable, genreTable, artistTable, albumTable, trackTable);
    }

    public FieldInformation getBigIntField(String name, int position) {
        return new FieldInformation(
                name,
                new ColumnTypeInformation( PolyType.BIGINT, null, PolyType.BIGINT.getMinPrecision(), PolyType.BIGINT.getMinScale(), -1, -1, false ),
                Collation.CASE_INSENSITIVE,
                PolyValue.fromType( 0, PolyType.BIGINT ),
                position
        );
    }

    public FieldInformation getDecimalField(String name, int position) {
        return new FieldInformation(
                name,
                new ColumnTypeInformation( PolyType.DECIMAL, null, 5, PolyType.DECIMAL.getMinScale(), -1, -1, false ),
                Collation.CASE_INSENSITIVE,
                PolyValue.fromType( 0.0, PolyType.DECIMAL ),
                position
        );
    }

    public FieldInformation getStringField(String name, int position) {
        return new FieldInformation(
                name,
                new ColumnTypeInformation( PolyType.VARCHAR, null, 10000, PolyType.VARCHAR.getMinScale(), -1, -1, false ),
                Collation.CASE_INSENSITIVE,
                PolyValue.fromType( "", PolyType.VARCHAR ),
                position
        );
    }
}
