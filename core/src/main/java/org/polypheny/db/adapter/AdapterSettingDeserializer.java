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

package org.polypheny.db.adapter;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

//see https://stackoverflow.com/questions/19588020/gson-serialize-a-list-of-polymorphic-objects/22081826#22081826
public class AdapterSettingDeserializer implements JsonDeserializer<AbstractAdapterSetting> {

    @Override
    public AbstractAdapterSetting deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();
        String type = jsonObject.get( "type" ).getAsString();
        String name = jsonObject.get( "name" ).getAsString();
        boolean canBeNull = jsonObject.get( "canBeNull" ).getAsBoolean();
        boolean required = jsonObject.get( "required" ).getAsBoolean();
        boolean modifiable = jsonObject.get( "modifiable" ).getAsBoolean();
        int position = jsonObject.get( "position" ).getAsInt();
        String description = null;
        if ( jsonObject.get( "description" ) != null ) {
            description = jsonObject.get( "description" ).getAsString();
        }

        AbstractAdapterSetting out;
        switch ( type ) {
            case "Integer":
                Integer integer = jsonObject.get( "defaultValue" ).getAsInt();
                out = new AbstractAdapterSettingInteger( name, canBeNull, required, modifiable, integer, new ArrayList<>(), position );
                break;
            case "String":
                String string = jsonObject.get( "defaultValue" ).getAsString();
                out = new AbstractAdapterSettingString( name, canBeNull, required, modifiable, string, new ArrayList<>(), position );
                break;
            case "Boolean":
                boolean bool = jsonObject.get( "defaultValue" ).getAsBoolean();
                out = new AbstractAdapterSettingBoolean( name, canBeNull, required, modifiable, bool, new ArrayList<>(), position );
                break;
            case "List":
                List<String> options = context.deserialize( jsonObject.get( "options" ), List.class );
                String defaultValue = context.deserialize( jsonObject.get( "defaultValue" ), String.class );
                out = new AbstractAdapterSettingList( name, canBeNull, required, modifiable, options, new ArrayList<>(), defaultValue, position );
                break;
            case "Directory":
                String directory = context.deserialize( jsonObject.get( "directory" ), String.class );
                String[] fileNames = context.deserialize( jsonObject.get( "fileNames" ), String[].class );
                out = new AbstractAdapterSettingDirectory( name, canBeNull, required, modifiable, new ArrayList<>(), position ).setDirectory( directory ).setFileNames( fileNames );
                break;
            default:
                throw new RuntimeException( "Could not deserialize AdapterSetting of type " + type );
        }
        out.setDescription( description );
        return out;
    }

}
