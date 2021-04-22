/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.monitoring.persistence;

import lombok.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PUBLIC)
@AllArgsConstructor(access = AccessLevel.MODULE)
public class QueryPersistentData implements MonitoringPersistentData, Serializable {

    private static final long serialVersionUID = 2312903042511293177L;

    private UUID Id;
    private String monitoringType;
    private String description;
    private long recordedTimestamp;
    private final List<String> tables = new ArrayList<>();
    private List<String> fieldNames;

    private final HashMap<String, Object> dataElements = new HashMap<>();


    @Override
    public UUID Id() {
        return this.Id;
    }

    @Override
    public long timestamp() {
        return this.recordedTimestamp;
    }
}



