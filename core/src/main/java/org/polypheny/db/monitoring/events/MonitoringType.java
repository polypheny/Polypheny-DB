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

package org.polypheny.db.monitoring.events;

import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.constant.Kind;

public enum MonitoringType {
    INSERT,
    TRUNCATE,
    DROP_COLUMN,
    DROP_TABLE,
    SET_ROW_COUNT,
    DELETE,
    SELECT,
    UPDATE,
    MINUS,
    INTERSECT,
    UNION;


    public static MonitoringType from( Kind kind ) {
        if ( EnumUtils.isValidEnum( MonitoringType.class, kind.name().toUpperCase() ) ) {
            return valueOf( kind.name().toUpperCase() );
        }
        throw new NotImplementedException();
    }
}
