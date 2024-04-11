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

package org.polypheny.db.catalog.util;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.IdBuilder;

public class LoggedIdBuilder {

    IdBuilder idBuilder = IdBuilder.getInstance();

    @Getter
    private final List<Long> logicals = new ArrayList<>();
    @Getter
    private final List<Long> fields = new ArrayList<>();
    @Getter
    private final List<Long> allocations = new ArrayList<>();
    @Getter
    private final List<Long> physicals = new ArrayList<>();


    public long getNewLogicalId() {
        long id = idBuilder.getNewLogicalId();
        logicals.add( id );
        return id;
    }


    public long getNewAllocId() {
        long id = idBuilder.getNewAllocId();
        allocations.add( id );
        return id;
    }


    public long getNewPhysicalId() {
        long id = idBuilder.getNewPhysicalId();
        physicals.add( id );
        return id;
    }


    public long getNewFieldId() {
        long id = idBuilder.getNewFieldId();
        fields.add( id );
        return id;
    }

}
