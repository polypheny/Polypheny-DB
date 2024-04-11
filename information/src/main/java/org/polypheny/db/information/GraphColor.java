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

package org.polypheny.db.information;


import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Possible colors for an InformationGraph
 */
public enum GraphColor {

    @JsonProperty("#F86C6B")
    PASTEL_RED,
    @JsonProperty("#20A8d8")
    BATTERY_CHARGED_BLUE,
    @JsonProperty("#FFC107")
    MIKADO_YELLOW,
    @JsonProperty("#21576A")
    POLICE_BLUE,
    @JsonProperty("#814848")
    TUSCAN_RED,
    @JsonProperty("#88BB9A")
    DARK_SEE_GREEN,
    @JsonProperty("#3A7C96")
    JELLY_BEAN_BLUE,
    @JsonProperty("#914661")
    TWILIGHT_LAVENDER,
    @JsonProperty("#BFA0AB")
    SILVER_PINK,
    @JsonProperty("#BAD80A")
    LIME;
}
