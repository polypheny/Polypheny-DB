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

package org.polypheny.db.adaptimizer.randomdata;

import java.text.ParseException;
import java.util.List;
import java.util.Random;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.util.Pair;

@AllArgsConstructor
@Getter
@Setter
public class DataColumnOption {

    private List<Object> providedData;

    private Boolean oneToOneRelation;

    private Boolean unique;

    private Float nullProbability;

    private Integer length;

    private Pair<Long, Long> longRange;

    private Pair<Double, Double> doubleRange;

    private Pair<Float, Float> floatRange;

    private Pair<Integer, Integer> intRange;

    private List<Object> uniformValues;


    public DataColumnOption() {
        this.providedData = null;
        this.oneToOneRelation = null;
        this.unique = false;
        this.nullProbability = null;
        this.length = null;
        this.longRange = null;
        this.doubleRange = null;
        this.floatRange = null;
        this.intRange = null;
        this.uniformValues = null;
    }


    public static DataColumnOption fromTemplate( DataColumnOption dataColumnOption ) {
        if ( dataColumnOption == null ) {
            return new DataColumnOption();
        }
        return new DataColumnOption(
                dataColumnOption.providedData,
                dataColumnOption.oneToOneRelation,
                dataColumnOption.unique,
                dataColumnOption.nullProbability,
                dataColumnOption.length,
                dataColumnOption.longRange,
                dataColumnOption.doubleRange,
                dataColumnOption.floatRange,
                dataColumnOption.intRange,
                dataColumnOption.uniformValues
        );
    }


    public void setDateRange( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( DataDefinitions.SDF_DATE.parse( start ).getTime(), DataDefinitions.SDF_DATE.parse( end ).getTime() );
    }


    public void setTimeRange( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( DataDefinitions.SDF_TIME.parse( start ).getTime(), DataDefinitions.SDF_TIME.parse( end ).getTime() );
    }


    public void setTimeRangeZ( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( DataDefinitions.SDF_TIME_Z.parse( start ).getTime(), DataDefinitions.SDF_TIME_Z.parse( end ).getTime() );
    }


    public void setTimestampRange( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( DataDefinitions.SDF_TIME_STAMP.parse( start ).getTime(), DataDefinitions.SDF_TIME_STAMP.parse( end ).getTime() );
    }


    public void setTimestampRangeZ( String start, String end ) throws ParseException {
        this.longRange = new Pair<>( DataDefinitions.SDF_TIME_STAMP_Z.parse( start ).getTime(), DataDefinitions.SDF_TIME_STAMP_Z.parse( end ).getTime() );
    }


    /**
     * Returns a random value from the referenced foreign key values.
     */
    public Object getNextReferencedValue( Random random ) {
        Object object = this.providedData.get( random.nextInt( this.providedData.size() ) );

        if ( this.oneToOneRelation ) {
            this.providedData.remove( object );
        }

        return object;
    }

}
