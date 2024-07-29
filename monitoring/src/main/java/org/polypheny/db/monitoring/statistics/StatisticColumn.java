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

package org.polypheny.db.monitoring.statistics;


import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;


/**
 * Stores the available statistic data of a specific column
 */

public abstract class StatisticColumn {


    public final long columnId;

    public final PolyType type;


    @Expose
    @Setter
    @Getter
    protected boolean full;

    @Expose
    @Getter
    @Setter
    protected List<PolyValue> uniqueValues = new ArrayList<>();

    @Expose
    @Getter
    @Setter
    protected PolyInteger count;


    public StatisticColumn( long columnId, PolyType type ) {
        this.columnId = columnId;
        this.type = type;
    }


    public abstract void insert( PolyValue val );

    public abstract void insert( List<PolyValue> values );

    public abstract String toString();


    public enum StatisticType {
        @SerializedName("temporal")
        TEMPORAL,
        @SerializedName("numeric")
        NUMERICAL,
        @SerializedName("alphabetic")
        ALPHABETICAL
    }

}
