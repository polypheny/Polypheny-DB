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

package org.polypheny.db.adaptimizer.rnddata;

import java.text.SimpleDateFormat;

public abstract class Options {
    // Data Definitions / Constants ----------------------------------
    public static final SimpleDateFormat SDF_DATE = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat SDF_TIME = new SimpleDateFormat("hh:mm:ss");
    public static final SimpleDateFormat SDF_TIME_Z = new SimpleDateFormat("hh:mm:ss z");
    public static final SimpleDateFormat SDF_TIME_STAMP = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    public static final SimpleDateFormat SDF_TIME_STAMP_Z = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss z");
    public static final Integer DEFAULT_VARCHAR_LENGTH = 10;
    // ---------------------------------------------------------------
}
