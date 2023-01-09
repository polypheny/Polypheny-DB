/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.extraction;

import com.google.gson.Gson;
import io.javalin.http.Context;

public class ServerControl {

    private Gson gson = new Gson();

    private ServerConfig serverConfig = new ServerConfig();

    public void getCurrentConfigAsJson(Context ctx) {
        String jsonInString = gson.toJson(serverConfig);
        ctx.result(jsonInString);
    }

    public void setSpeedThoroughness(int speedThoroughness) {
        serverConfig.setSpeedThoroughness(speedThoroughness);
    }

}

class ServerConfig {
    public int speedThoroughness;

    ServerConfig() {
        speedThoroughness = 0;
    }

    public void setSpeedThoroughness(int speedThoroughness) {
        this.speedThoroughness = speedThoroughness;
    }
}