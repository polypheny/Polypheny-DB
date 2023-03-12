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

package org.polypheny.db.polyfier.data;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Slf4j
@AllArgsConstructor
@Getter
@Setter
public class ColumnOption {

    private boolean modified;

    private PolyType type;

    private String columnName;

    private Long referencedColumn;

    private Long catalogColumn;

    private Set<Long> referencedBy;

    private boolean unique;

    private Float nullProbability;

    private Integer length;

    private Pair<Long, Long> longRange;

    private Pair<Double, Double> doubleRange;

    private Pair<Float, Float> floatRange;

    private Pair<Integer, Integer> intRange;

    private List<Object> uniformValues;

    // stub -----------------------------------------
    // Todo add foreign key routines
    private Function2<Object, Triple<Long, Random, Integer>, Object> fkRoutine;

    private boolean foreignKeyRoutine;
    // stub -----------------------------------------

    public ColumnOption( Long column ) {
        this.catalogColumn = column;
        this.referencedColumn = null;
        this.referencedBy = new HashSet<>();
        this.unique = false;
        this.nullProbability = null;
        this.length = null;
        this.longRange = null;
        this.doubleRange = null;
        this.floatRange = null;
        this.intRange = null;
        this.uniformValues = null;
        this.fkRoutine = null;
        this.foreignKeyRoutine = false;
    }


    public void setDateRange( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( Options.SDF_DATE.parse( start ).getTime(), Options.SDF_DATE.parse( end ).getTime() );
    }


    public void setTimeRange( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( Options.SDF_TIME.parse( start ).getTime(), Options.SDF_TIME.parse( end ).getTime() );
    }


    public void setTimeRangeZ( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( Options.SDF_TIME_Z.parse( start ).getTime(), Options.SDF_TIME_Z.parse( end ).getTime() );
    }


    public void setTimestampRange( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( Options.SDF_TIME_STAMP.parse( start ).getTime(), Options.SDF_TIME_STAMP.parse( end ).getTime() );
    }


    public void setTimestampRangeZ( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( Options.SDF_TIME_STAMP_Z.parse( start ).getTime(), Options.SDF_TIME_STAMP_Z.parse( end ).getTime() );
    }

}
