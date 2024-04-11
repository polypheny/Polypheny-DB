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

import java.util.Arrays;
import java.util.List;
import lombok.experimental.Accessors;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterSettingList;

@Accessors(chain = true)
public class AbstractAdapterSettingList extends AbstractAdapterSetting {

    public List<String> options;
    public boolean dynamic = false;


    public AbstractAdapterSettingList( String name, boolean canBeNull, final String subOf, boolean required, boolean modifiable, List<String> options, List<DeploySetting> modes, String defaultValue, int position ) {
        super( AdapterSettingType.LIST, name, canBeNull, subOf, required, modifiable, modes, defaultValue, position );
        this.options = options;
    }


    public static AbstractAdapterSetting fromAnnotation( AdapterSettingList annotation ) {
        return new AbstractAdapterSettingList(
                annotation.name(),
                annotation.canBeNull(),
                annotation.subOf(),
                annotation.required(),
                annotation.modifiable(),
                Arrays.asList( annotation.options() ),
                Arrays.asList( annotation.appliesTo() ),
                annotation.defaultValue(),
                annotation.position() );
    }


    @Override
    public String getValue() {
        return defaultValue;
    }

}
