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

package org.polypheny.db.backup.manifest;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.logistic.DataModel;

@Getter @Setter
public class EntityInfo {
    private List<String> filePaths;
    private String entityName;
    private String namespaceName;
    private Long namespaceId;
    private DataModel dataModel;
    private int nbrCols;
    private String checksum;


    public EntityInfo( List<String> filePaths, String entityName, String namespaceName, Long namespaceId, DataModel dataModel ) {
        this.filePaths = filePaths;
        this.entityName = entityName;
        this.namespaceName = namespaceName;
        this.namespaceId = namespaceId;
        this.dataModel = dataModel;
    }

    public EntityInfo( List<String> filePaths, String entityName, String namespaceName, Long namespaceId, DataModel dataModel, int nbrCols ) {
        this.filePaths = filePaths;
        this.entityName = entityName;
        this.namespaceName = namespaceName;
        this.namespaceId = namespaceId;
        this.dataModel = dataModel;
        this.nbrCols = nbrCols;
    }

}
