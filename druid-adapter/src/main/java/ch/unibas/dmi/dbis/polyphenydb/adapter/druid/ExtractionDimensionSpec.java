/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;

import static ch.unibas.dmi.dbis.polyphenydb.adapter.druid.DruidQuery.writeField;
import static ch.unibas.dmi.dbis.polyphenydb.adapter.druid.DruidQuery.writeFieldIf;


/**
 * Implementation of extraction function DimensionSpec.
 *
 * The extraction function implementation returns dimension values transformed using the given extraction function.
 */
public class ExtractionDimensionSpec implements DimensionSpec {

    private final String dimension;
    private final ExtractionFunction extractionFunction;
    private final String outputName;
    private final DruidType outputType;


    public ExtractionDimensionSpec( String dimension, ExtractionFunction extractionFunction, String outputName ) {
        this( dimension, extractionFunction, outputName, DruidType.STRING );
    }


    public ExtractionDimensionSpec( String dimension, ExtractionFunction extractionFunction, String outputName, DruidType outputType ) {
        this.dimension = Objects.requireNonNull( dimension );
        this.extractionFunction = Objects.requireNonNull( extractionFunction );
        this.outputName = outputName;
        this.outputType = outputType == null ? DruidType.STRING : outputType;
    }


    @Override
    public String getOutputName() {
        return outputName;
    }


    @Override
    public DruidType getOutputType() {
        return outputType;
    }


    @Override
    public ExtractionFunction getExtractionFn() {
        return extractionFunction;
    }


    @Override
    public String getDimension() {
        return dimension;
    }


    @Override
    public void write( JsonGenerator generator ) throws IOException {
        generator.writeStartObject();
        generator.writeStringField( "type", "extraction" );
        generator.writeStringField( "dimension", dimension );
        writeFieldIf( generator, "outputName", outputName );
        writeField( generator, "extractionFn", extractionFunction );
        generator.writeEndObject();
    }


    /**
     * @param dimensionSpec Druid Dimesion spec object
     * @return valid {@link Granularity} of floor extract or null when not possible.
     */
    @Nullable
    public static Granularity toQueryGranularity( DimensionSpec dimensionSpec ) {
        if ( !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals( dimensionSpec.getDimension() ) ) {
            // Only __time column can be substituted by granularity
            return null;
        }
        final ExtractionFunction extractionFunction = dimensionSpec.getExtractionFn();
        if ( extractionFunction == null ) {
            // No Extract thus no Granularity
            return null;
        }
        if ( extractionFunction instanceof TimeExtractionFunction ) {
            Granularity granularity = ((TimeExtractionFunction) extractionFunction).getGranularity();
            String format = ((TimeExtractionFunction) extractionFunction).getFormat();
            if ( !TimeExtractionFunction.ISO_TIME_FORMAT.equals( format ) ) {
                return null;
            }
            return granularity;
        }
        return null;
    }

}

