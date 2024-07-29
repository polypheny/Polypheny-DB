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

package org.polypheny.db.webui.models.requests;


import lombok.AllArgsConstructor;
import lombok.Value;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition;


/**
 * Model for a request to edit or create a Table used for request where you want to truncate/drop a table
 * and when you want to create a new table
 */
@Value
@AllArgsConstructor
public class EditTableRequest {

    public @Nullable Long namespaceId;
    public @Nullable Long entityId;
    public @Nullable String entityName;

    public String action; // truncate / drop
    public UiColumnDefinition[] columns;
    public Long storeId;
    public EntityType tableType;



}
