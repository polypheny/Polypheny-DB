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

package org.polypheny.db.adapter;

import java.util.Arrays;
import java.util.List;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterSettingString;

public class AbstractAdapterSettingString extends AbstractAdapterSetting {

    private final String type = "String";


    public AbstractAdapterSettingString( String name, boolean canBeNull, boolean required, boolean modifiable, String defaultValue, List<DeploySetting> modes, int position ) {
        super( name, canBeNull, required, modifiable, modes, defaultValue, position );
    }


    public static AbstractAdapterSetting fromAnnotation( AdapterSettingString annotation ) {
        return new AbstractAdapterSettingString(
                annotation.name(),
                annotation.canBeNull(),
                annotation.required(),
                annotation.modifiable(),
                annotation.defaultValue(),
                Arrays.asList( annotation.appliesTo() ),
                annotation.position() );
    }


    @Override
    public String getValue() {
        return defaultValue;
    }

}
