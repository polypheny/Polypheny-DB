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

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class SourceModel extends AdapterModel {

    public boolean readOnly;


    public SourceModel(
            @Nullable Long id,
            @Nullable String name,
            String adapterName,
            AdapterType type,
            Map<String, AdapterSettingValueModel> settings,
            DeployMode mode,
            boolean readOnly ) {
        super( id, name, adapterName, type, settings, mode );
        this.readOnly = readOnly;
    }

}
