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

package org.polypheny.db.adapter.googlesheet;


import java.util.HashMap;
import java.util.Map;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.Adapter;

public class GoogleSheetPlugin extends Plugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public GoogleSheetPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        Map<String, String> settings = new HashMap<>();
        settings.put( "maxStringLength", "255" );
        settings.put( "querySize", "1000" );
        settings.put( "sheetsURL", "" );
        settings.put( "mode", "remote" );
        settings.put( "resetRefreshToken", "No" );

        Adapter.addAdapter( GoogleSheetSource.class, "GOOGLESHEETS", settings );
    }

}
