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

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;

@Accessors(chain = true)
public class AbstractAdapterSettingDirectory extends AbstractAdapterSetting {

    @Setter
    public String directory;
    //This field is necessary for the UI and needs to be initialized to be serialized to JSON.
    @Setter
    public String[] fileNames = new String[]{ "" };
    public transient final Map<String, InputStream> inputStreams;


    public AbstractAdapterSettingDirectory( String name, String defaultValue, boolean canBeNull, String subOf, boolean required, boolean modifiable, List<DeploySetting> modes, int position ) {
        super( AdapterSettingType.DIRECTORY, name, canBeNull, subOf, required, modifiable, modes, defaultValue, position );
        //so it will be serialized
        this.directory = "";
        this.inputStreams = new HashMap<>();
    }


    public static AbstractAdapterSetting fromAnnotation( AdapterSettingDirectory annotation ) {
        return new AbstractAdapterSettingDirectory(
                annotation.name(),
                annotation.defaultValue(),
                annotation.canBeNull(),
                annotation.subOf(),
                annotation.required(),
                annotation.modifiable(),
                Arrays.asList( annotation.appliesTo() ),
                annotation.position()
        );
    }


    @Override
    public String getValue() {
        return directory;
    }

}
