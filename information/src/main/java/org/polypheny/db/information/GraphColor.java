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

package org.polypheny.db.information;


import com.google.gson.annotations.SerializedName;

/**
 * Possible colors for an InformationGraph
 */
public enum GraphColor {

    @SerializedName("#F86C6B")
    PASTEL_RED,
    @SerializedName("#20A8d8")
    BATTERY_CHARGED_BLUE,
    @SerializedName("#FFC107")
    MIKADO_YELLOW,
    @SerializedName("#21576A")
    POLICE_BLUE,
    @SerializedName("#814848")
    TUSCAN_RED,
    @SerializedName("#88BB9A")
    DARK_SEE_GREEN,
    @SerializedName("#3A7C96")
    JELLY_BEAN_BLUE,
    @SerializedName("#914661")
    TWILIGHT_LAVENDER,
    @SerializedName("#BFA0AB")
    SILVER_PINK,
    @SerializedName("#BAD80A")
    LIME
}
