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

package org.polypheny.db.webui.models;


import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSetting.AdapterSettingType;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

@Value
public class AdapterTemplateModel {


    public String adapterName;
    public AdapterType adapterType;
    public List<AdapterSettingsModel> defaultSettings;
    public String description;
    public List<DeployMode> modes;


    public AdapterTemplateModel(
            @NotNull String adapterName,
            @NotNull AdapterType adapterType,
            @NotNull List<AdapterSettingsModel> defaultSettings,
            @NotNull String description,
            @NotNull List<DeployMode> modes ) {
        this.adapterName = adapterName;
        this.adapterType = adapterType;
        this.defaultSettings = defaultSettings;
        this.description = description;
        this.modes = modes;
    }


    public static AdapterTemplateModel from( AdapterInformation template ) {
        return new AdapterTemplateModel(
                template.name,
                template.type,
                template.settings.stream().map( AdapterSettingsModel::from ).collect( Collectors.toList() ),
                template.description,
                template.modes );
    }


    @Value
    public static class AdapterSettingsModel {


        public String name;
        public String defaultValue;
        public String description;
        public List<DeploySetting> appliesTo;
        public boolean required;
        public boolean canBeNull;
        public AdapterSettingType type;


        public AdapterSettingsModel( AdapterSettingType type, String name, String defaultValue, String description, List<DeploySetting> appliesTo, boolean required, boolean canBeNull ) {
            this.type = type;
            this.name = name;
            this.defaultValue = defaultValue;
            this.description = description;
            this.appliesTo = appliesTo;
            this.required = required;
            this.canBeNull = canBeNull;
        }


        public static AdapterSettingsModel from( AbstractAdapterSetting setting ) {
            return new AdapterSettingsModel(
                    setting.type,
                    setting.name,
                    setting.defaultValue,
                    setting.description,
                    setting.appliesTo,
                    setting.required,
                    setting.canBeNull );
        }

    }

}
