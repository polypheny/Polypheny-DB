/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.local.LocalFs;

/**
 * Creates Hadoop configurations with the correct classloader and
 * built-in local filesystem registrations for plugin environments.
 */
public final class HadoopConfigurationFactory {

    private HadoopConfigurationFactory() {
    }

    public static Configuration create( ClassLoader classLoader ) {
        Configuration conf = new Configuration( false );
        conf.setClassLoader( classLoader );
        conf.set( "fs.file.impl", LocalFileSystem.class.getName() );
        conf.set( "fs.AbstractFileSystem.file.impl", LocalFs.class.getName() );
        return conf;
    }

}
