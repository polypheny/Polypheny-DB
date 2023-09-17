/*
 * Copyright 2019-2023 The Polypheny Project
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


import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
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
    public SortState sort;
    public String filter;
    public Long id;

    // for editing columns
    public boolean primary;
    public boolean nullable;
    public Integer precision;
    public Integer scale;
    public String defaultValue;
    public Integer dimension;
    public Integer cardinality;
    public String collectionsType;

    //for data source columns
    public String as;


}
