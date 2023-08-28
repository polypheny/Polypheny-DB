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


import java.util.List;
import lombok.Getter;
import lombok.Value;


/**
 * Schema for the index of a table
 */
@Getter
@Value
public class IndexModel {

    public Long namespaceId;
    public Long entityId;
    public String name;
    public String storeUniqueName;
    public String method;
    public List<Long> columnIds;


    public IndexModel( final Long namespaceId, final Long entityId, final String name, final String method, final List<Long> columnIds ) {
        this.namespaceId = namespaceId;
        this.entityId = entityId;
        this.name = name;
        this.method = method;
        this.columnIds = columnIds;
        this.storeUniqueName = null;
    }


}
