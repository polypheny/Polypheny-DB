/*
 * Copyright 2019-2020 The Polypheny Project
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


import lombok.Builder;
import lombok.Getter;


/**
 * Model for a ForeignKey
 */
@Getter
@Builder
public class ForeignKey {

    private final String fkName;

    private final String targetSchema;
    private final String targetTable;
    private final String targetColumn;

    private final String sourceSchema;
    private final String sourceTable;
    private final String sourceColumn;

    private final String onUpdate;
    private final String onDelete;

}
