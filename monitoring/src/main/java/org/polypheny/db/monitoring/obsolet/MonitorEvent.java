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

package org.polypheny.db.monitoring.obsolet;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;

import java.io.Serializable;
import java.util.List;


@Getter
@Builder
public class MonitorEvent implements Serializable {


    private static final long serialVersionUID = 2312903042511293177L;

    public String monitoringType;
    private String description;
    private List<String> fieldNames;
    private long recordedTimestamp;
    private RelRoot routed;
    private PolyphenyDbSignature signature;
    private Statement statement;
    private List<List<Object>> rows;
    @Setter
    private RelOptTable table;


}
