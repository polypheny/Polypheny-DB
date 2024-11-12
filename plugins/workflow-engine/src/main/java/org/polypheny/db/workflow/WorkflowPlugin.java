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

package org.polypheny.db.workflow;


import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;

public class WorkflowPlugin extends PolyPlugin {

    private WorkflowManager manager;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public WorkflowPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        System.out.println( "WorkflowPlugin: after catalog init" );
        manager = new WorkflowManager();
    }


    @Override
    public void stop() {
        System.out.println( "WorkflowPlugin: on stop" );
    }

}
