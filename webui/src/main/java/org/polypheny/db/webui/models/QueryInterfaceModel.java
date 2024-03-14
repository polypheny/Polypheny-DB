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


import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterface.QueryInterfaceSetting;


/**
 * A model for a {@link org.polypheny.db.iface.QueryInterface}
 * needed for gson
 */
@Value
@AllArgsConstructor
public class QueryInterfaceModel {

    public String uniqueName;
    public boolean supportsDdl;
    public boolean supportsDml;
    public String interfaceType;
    public Map<String, String> currentSettings;
    public QueryInterfaceSetting[] availableSettings;

    public QueryInterfaceModel ( final QueryInterface i ) {
        this.uniqueName = i.getUniqueName();
        this.interfaceType = i.getInterfaceType();
        this.currentSettings = i.getCurrentSettings();
        this.availableSettings = i.getAvailableSettings().toArray( new QueryInterfaceSetting[0] );
        this.supportsDdl = i.isSupportsDdl();
        this.supportsDml = i.isSupportsDml();
    }

}
