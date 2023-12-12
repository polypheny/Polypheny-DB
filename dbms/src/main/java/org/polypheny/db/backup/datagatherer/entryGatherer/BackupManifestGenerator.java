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

package org.polypheny.db.backup.datagatherer.entryGatherer;

import com.google.gson.Gson;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.monitoring.exceptions.GenericEventAnalyzeRuntimeException;

public class BackupManifestGenerator {


    public static void generateManifest( List<EntityInfo> entityInfoList, String overallChecksum, String manifestPath) {
        BackupManifest backupManifest = new BackupManifest(entityInfoList, overallChecksum);
        Gson gson = new Gson();
        String json = gson.toJson(backupManifest);

        try (FileWriter writer = new FileWriter(manifestPath);)
        {
            writer.write(json);
            writer.flush();
        } catch ( IOException e) {
            throw new GenericRuntimeException("Error while writing manifest file" + e.getMessage());
        }
    }

}
