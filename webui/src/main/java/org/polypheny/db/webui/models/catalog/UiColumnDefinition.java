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

package org.polypheny.db.webui.models.catalog;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.webui.models.SortState;


/**
 * Information about a column of a table for the header of a table in the UI
 */
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
@Jacksonized
@SuperBuilder
@Value
public class UiColumnDefinition extends FieldDefinition {

    // for the Data-Table in the UI
    @JsonProperty
    public SortState sort;
    @JsonProperty
    public String filter;
    @JsonProperty
    public Long id;

    // for editing columns
    @JsonProperty
    public boolean primary;
    @JsonProperty
    public boolean nullable;
    @JsonProperty
    public Integer precision;
    @JsonProperty
    public Integer scale;
    @JsonProperty
    public String defaultValue;
    @JsonProperty
    public Integer dimension;
    @JsonProperty
    public Integer cardinality;
    @JsonProperty
    @Nullable
    public String collectionsType;

    //for data source columns
    @JsonProperty
    public String as;


}
