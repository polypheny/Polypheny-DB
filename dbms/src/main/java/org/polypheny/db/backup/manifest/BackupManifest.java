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


import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class BackupManifest {
    private List<EntityInfo> entityInfos;
    private String overallChecksum;
    private Date backupDate;

    public BackupManifest( List<EntityInfo> entityInfos, String overallChecksum, Date backupDate ) {
        this.entityInfos = entityInfos;
        this.overallChecksum = overallChecksum;
        this.backupDate = backupDate;
    }

}
