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

package org.polypheny.db.workflow.dag.variables;

import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.workflow.dag.settings.SettingValue;

public class VariableStore implements ReadableVariableStore, WritableVariableStore {

    private final Map<String, SettingValue> variables = new HashMap<>();


    public ReadableVariableStore asReadable() {
        return this;
    }


    public WritableVariableStore asWritable() {
        return this;
    }


    @Override
    public void setVariable( String key, SettingValue value ) {
        variables.put( key, value );
    }


    @Override
    public void merge( ReadableVariableStore newStore ) {
        variables.putAll( newStore.getVariables() );
    }


    @Override
    public void clear() {
        variables.clear();
    }


    @Override
    public boolean contains( String key ) {
        return variables.containsKey( key );
    }


    @Override
    public SettingValue getVariable( String key ) {
        return variables.get( key );
    }


    @Override
    public Map<String, SettingValue> getVariables() {
        return Map.copyOf( variables );
    }

}
