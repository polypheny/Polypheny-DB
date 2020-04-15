/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.catalog;


import java.io.File;


/**
 * The resourceManager should handle all dynamically needed resources
 */
public class ResourceManager {

    static ResourceManager resourceManager = null;
    final String root = "./resources";


    public static ResourceManager getInstance() {
        if( resourceManager == null ) {
            resourceManager = new ResourceManager();
        }
        return resourceManager;
    }

    private ResourceManager(){
        File root = new File( this.root );
        if( !root.exists() ) {
            root.mkdir();
        }
    }

    public String registerDataFolder( String path ) {
        File root = new File( this.root + "\\" + path);
        if( !root.exists() ) {
            root.mkdir();
        }

        return this.root + "\\" + path + "\\";
    }

}
