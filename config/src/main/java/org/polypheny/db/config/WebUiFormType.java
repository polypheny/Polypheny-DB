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

package org.polypheny.db.config;


/**
 * The type of the config for the WebUi to specify how it should be rendered in the UI (&lt;input type="text/number/etc."&gt;) e.g. text or number
 */
public enum WebUiFormType {
    TEXT( "text" ),
    NUMBER( "number" ),
    BOOLEAN( "boolean" ),
    SELECT( "select" ),
    CHECKBOXES( "checkboxes" ),
    LIST( "list" ),
    DOCKER_INSTANCE( "docker" ),
    PLUGIN_INSTANCE( "plugin" );

    private final String type;


    WebUiFormType( final String t ) {
        this.type = t;
    }


    @Override
    public String toString() {
        return this.type;
    }
}