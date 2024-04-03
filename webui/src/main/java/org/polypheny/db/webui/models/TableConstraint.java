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

package org.polypheny.db.webui.models;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.StringJoiner;
import lombok.Value;
import lombok.experimental.NonFinal;


@Value
public class TableConstraint {

    @JsonProperty
    public String name;

    @JsonProperty
    public String type;

    @NonFinal
    public boolean deferrable;

    @NonFinal
    public boolean initially_deferred;

    @JsonProperty
    public String[] columns;


    @JsonCreator
    public TableConstraint( @JsonProperty("name") final String name, @JsonProperty("type") final String type, @JsonProperty("columns") List<String> columns ) {
        this.name = name;
        this.type = type;
        this.columns = columns.toArray( new String[0] );
    }


    public String[] asRow() {
        StringJoiner joiner = new StringJoiner( ", " );
        for ( String column : columns ) {
            joiner.add( column );
        }
        return new String[]{ this.name, this.type, joiner.toString() };
    }
}
