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

<<<<<<<< HEAD:core/src/main/java/org/polypheny/db/plugins/PluginContext.java
package org.polypheny.db.plugins;

import org.pf4j.RuntimeMode;

public class PluginContext {

    private final RuntimeMode runtimeMode;


    public PluginContext( RuntimeMode runtimeMode ) {
        this.runtimeMode = runtimeMode;
    }


    public RuntimeMode getRuntimeMode() {
        return runtimeMode;
    }
========
package org.polypheny.db.catalog.catalogs;

public interface PhysicalCatalog {
>>>>>>>> f595385dd (added logistic PolyCatalog functions):core/src/main/java/org/polypheny/db/catalog/catalogs/PhysicalCatalog.java

}
