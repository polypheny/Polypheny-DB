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
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSetting.AdapterSettingType;
import org.polypheny.db.adapter.AbstractAdapterSettingList;
import org.polypheny.db.adapter.BindableAbstractAdapterSettingsList;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;

public record AdapterTemplateModel( @JsonProperty String adapterName, @JsonProperty AdapterType adapterType, @JsonProperty List<AdapterSettingsModel> settings, @JsonProperty String description, @JsonProperty List<DeployMode> modes ) {


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
            settings = template
                    .settings
                    .stream()
                    .map( m -> m.name.equals( "instanceId" ) ?
                            new BindableAbstractAdapterSettingsList<>( m.name, m.name, m.canBeNull, m.subOf, m.required, m.modifiable,
                                    RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class ), m.appliesTo, ConfigDocker::getAlias, ConfigDocker.class )
                            : m )
                    .map( AdapterSettingsModel::from ).toList();
        }

        return new AdapterTemplateModel(
                template.adapterName,
                template.adapterType,
                settings,
                template.description,
                template.modes );
    }


    public record AdapterSettingsModel(
            @JsonProperty String type,
            @JsonProperty String subOf,
            @JsonProperty String name,
            @JsonProperty String nameAlias,
            @JsonProperty Map<String, String> alias,
            @JsonProperty String description,
            @JsonProperty String defaultValue,
            @JsonProperty boolean canBeNull,
            @JsonProperty boolean required,
            @JsonProperty boolean modifiable,
            @JsonProperty List<String> options,
            @JsonProperty List<String> fileNames,
            @JsonProperty Boolean dynamic,
            @JsonProperty Integer position,
            @JsonProperty List<DeploySetting> appliesTo
    ) {

        public static AdapterSettingsModel from( AbstractAdapterSetting setting ) {
            BindableAbstractAdapterSettingsList<ConfigDocker> s = null;
            if ( setting instanceof BindableAbstractAdapterSettingsList<?> b && b.getClazz() == ConfigDocker.class ) {
                s = (BindableAbstractAdapterSettingsList<ConfigDocker>) setting;
            }
            return new AdapterSettingsModel(
                    setting.type.toString(),
                    setting.subOf,
                    setting.name,
                    s != null
                            ? s.getNameAlias()
                            : null,
                    s != null
                            ? s.getAlias()
                            : null,
                    setting.description,
                    setting.defaultValue,
                    setting.canBeNull,
                    setting.required,
                    setting.modifiable,
                    setting.type == AdapterSettingType.LIST
                            ? ((AbstractAdapterSettingList) setting).options
                            : null,
                    List.of(),
                    null,
                    null,
                    setting.appliesTo
            );
        }

    }

}
