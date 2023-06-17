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

package org.polypheny.db.type.entity.category;

import java.util.Calendar;
import java.util.GregorianCalendar;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public abstract class PolyTemporal extends PolyValue {

    public abstract Long getSinceEpoch();


    public PolyTemporal( PolyType type ) {
        super( type );
    }


    public Calendar toCalendar() {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis( getSinceEpoch() );
        return cal;
    }

}
