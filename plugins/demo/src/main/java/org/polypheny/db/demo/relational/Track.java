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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.polypheny.db.demo.IPreperable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Track implements IPreperable {
    @JsonProperty("TrackId")
    public int trackId;

    @JsonProperty("Name")
    public String name;

    @JsonProperty("AlbumId")
    public int albumId;

    @JsonProperty("MediaTypeId")
    public int mediaTypeId;

    @JsonProperty("GenreId")
    public int genreId;

    @JsonProperty("Composer")
    public String composer;

    @JsonProperty("Milliseconds")
    public long milliseconds;

    @JsonProperty("Bytes")
    public long bytes;

    @JsonProperty("UnitPrice")
    public float unitPrice;


    @Override
    public boolean filter() {
        return this.name == null || this.composer == null;
    }


    @Override
    public void setValues( PreparedStatement statement ) throws SQLException {
        statement.setInt( 1, this.trackId );
        statement.setString( 2, this.name );
        statement.setInt( 3, this.albumId );
        statement.setInt( 4, this.mediaTypeId );
        statement.setInt( 5, this.genreId );
        statement.setString( 6, this.composer );
        statement.setLong( 7, this.milliseconds );
        statement.setLong( 8, this.bytes );
        statement.setFloat( 9, this.unitPrice );
    }

}
