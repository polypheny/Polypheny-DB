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

package org.polypheny.db.webui.models;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSetting.AdapterSettingType;
import org.polypheny.db.adapter.AbstractAdapterSettingList;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.docker.DockerManager;

public record AdapterTemplateModel(@JsonProperty String adapterName, @JsonProperty AdapterType adapterType, @JsonProperty List<AdapterSettingsModel> settings, @JsonProperty String description, @JsonProperty List<DeployMode> modes) {


    public AdapterTemplateModel(
            @NotNull String adapterName,
            @NotNull AdapterType adapterType,
            @NotNull List<AdapterSettingsModel> settings,
            @NotNull String description,
            @NotNull List<DeployMode> modes ) {
        this.adapterName = adapterName;
        this.adapterType = adapterType;
        this.settings = settings;
        this.description = description;
        this.modes = modes;
    }


    public static AdapterTemplateModel from( AdapterTemplate template ) {
        List<AdapterSettingsModel> settings = template.settings.stream().map( AdapterSettingsModel::from ).toList();
        if ( template.modes.contains( DeployMode.DOCKER ) ) {
            List<String> ids = DockerManager.getInstance().getDockerInstances().keySet().stream().map( Object::toString ).toList();
            settings = template
                    .settings
                    .stream()
                    .map( AdapterSettingsModel::from )
                    .map( m -> m.name.equals( "instanceId" ) ? new AdapterSettingsModel( m.type, m.name, ids.isEmpty() ? "0" : ids.get( 0 ), m.description, m.appliesTo, ids, m.required, m.canBeNull, m.subOf, List.of() ) : m ).toList();
        }

        return new AdapterTemplateModel(
                template.adapterName,
                template.adapterType,
                settings,
                template.description,
                template.modes );
    }


    public record AdapterSettingsModel(@JsonProperty AdapterSettingType type, @JsonProperty String name, @JsonProperty String defaultValue, @JsonProperty String description, @JsonProperty List<DeploySetting> appliesTo, @JsonProperty List<String> options, @JsonProperty boolean required, @JsonProperty boolean canBeNull, @JsonProperty String subOf, @JsonProperty List<String> fileNames) {


        public static AdapterSettingsModel from( AbstractAdapterSetting setting ) {
            return new AdapterSettingsModel(
                    setting.type,
                    setting.name,
                    setting.defaultValue,
                    setting.description,
                    setting.appliesTo,
                    setting.type == AdapterSettingType.LIST ? ((AbstractAdapterSettingList) setting).options : null,
                    setting.required,
                    setting.canBeNull,
                    setting.subOf,
                    List.of() );
        }

    }

}
