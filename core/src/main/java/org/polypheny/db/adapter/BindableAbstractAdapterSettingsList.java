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

package org.polypheny.db.adapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.experimental.Accessors;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigObject;
import org.polypheny.db.config.RuntimeConfig;

/**
 * BindableSettingsList which allows to configure mapped AdapterSettings, which expose an alias in the frontend
 * but assign a corresponding id when the value is chosen
 *
 * @param <T>
 */
@Accessors(chain = true)
public class BindableAbstractAdapterSettingsList<T extends ConfigObject> extends AbstractAdapterSettingList {

    private final transient Function<T, String> mapper;
    private final transient Class<T> clazz;
    private Map<Integer, String> alias;
    private final String nameAlias;
    public RuntimeConfig boundConfig;


    public BindableAbstractAdapterSettingsList( String name, String nameAlias, boolean canBeNull, String subOf, boolean required, boolean modifiable, List<T> options, Function<T, String> mapper, Class<T> clazz ) {
        super( name, canBeNull, subOf, required, modifiable, options.stream().map( ( el ) -> String.valueOf( el.getId() ) ).collect( Collectors.toList() ), new ArrayList<>(), null, 1000 );
        this.mapper = mapper;
        this.clazz = clazz;
        this.dynamic = true;
        this.nameAlias = nameAlias;
        this.alias = options.stream().collect( Collectors.toMap( ConfigObject::getId, mapper ) );
    }


    /**
     * This allows to bind this option to an existing RuntimeConfig,
     * which will update when the bound option changes
     *
     * @param config the RuntimeConfig which is bound
     * @return chain method to use the object
     */
    public AbstractAdapterSetting bind( RuntimeConfig config ) {
        this.boundConfig = config;
        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                refreshFromConfig();
            }


            @Override
            public void restart( Config c ) {
                refreshFromConfig();
            }
        };
        config.addObserver( listener );

        return this;
    }


    public void refreshFromConfig() {
        if ( boundConfig != null ) {
            options = boundConfig.getList( clazz ).stream().map( ( el ) -> String.valueOf( el.id ) ).collect( Collectors.toList() );
            alias = boundConfig.getList( clazz ).stream().collect( Collectors.toMap( ConfigObject::getId, mapper ) );
        }
    }


}
