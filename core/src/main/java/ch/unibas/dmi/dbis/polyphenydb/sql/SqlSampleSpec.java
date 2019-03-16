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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;


/**
 * Specification of a SQL sample.
 *
 * For example, the query
 *
 * <blockquote>
 * <pre>SELECT * FROM emp TABLESAMPLE SUBSTITUTE('medium')</pre>
 * </blockquote>
 *
 * declares a sample which is created using {@link #createNamed}.
 *
 * A sample is not a {@link SqlNode}. To include it in a parse tree, wrap it as a literal, viz:
 * {@link SqlLiteral#createSample(SqlSampleSpec, ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos)}.
 */
public abstract class SqlSampleSpec {

    protected SqlSampleSpec() {
    }


    /**
     * Creates a sample which substitutes one relation for another.
     */
    public static SqlSampleSpec createNamed( String name ) {
        return new SqlSubstitutionSampleSpec( name );
    }


    /**
     * Creates a table sample without repeatability.
     *
     * @param isBernoulli true if Bernoulli style sampling is to be used; false for implementation specific sampling
     * @param samplePercentage likelihood of a row appearing in the sample
     */
    public static SqlSampleSpec createTableSample( boolean isBernoulli, float samplePercentage ) {
        return new SqlTableSampleSpec( isBernoulli, samplePercentage );
    }


    /**
     * Creates a table sample with repeatability.
     *
     * @param isBernoulli true if Bernoulli style sampling is to be used; false for implementation specific sampling
     * @param samplePercentage likelihood of a row appearing in the sample
     * @param repeatableSeed seed value used to reproduce the same sample
     */
    public static SqlSampleSpec createTableSample( boolean isBernoulli, float samplePercentage, int repeatableSeed ) {
        return new SqlTableSampleSpec( isBernoulli, samplePercentage, repeatableSeed );
    }


    /**
     * Sample specification that orders substitution.
     */
    public static class SqlSubstitutionSampleSpec extends SqlSampleSpec {

        private final String name;


        private SqlSubstitutionSampleSpec( String name ) {
            this.name = name;
        }


        public String getName() {
            return name;
        }


        public String toString() {
            return "SUBSTITUTE(" + PolyphenyDbSqlDialect.DEFAULT.quoteStringLiteral( name ) + ")";
        }
    }


    /**
     * Sample specification.
     */
    public static class SqlTableSampleSpec extends SqlSampleSpec {

        private final boolean isBernoulli;
        private final float samplePercentage;
        private final boolean isRepeatable;
        private final int repeatableSeed;


        private SqlTableSampleSpec( boolean isBernoulli, float samplePercentage ) {
            this.isBernoulli = isBernoulli;
            this.samplePercentage = samplePercentage;
            this.isRepeatable = false;
            this.repeatableSeed = 0;
        }


        private SqlTableSampleSpec( boolean isBernoulli, float samplePercentage, int repeatableSeed ) {
            this.isBernoulli = isBernoulli;
            this.samplePercentage = samplePercentage;
            this.isRepeatable = true;
            this.repeatableSeed = repeatableSeed;
        }


        /**
         * Indicates Bernoulli vs. System sampling.
         */
        public boolean isBernoulli() {
            return isBernoulli;
        }


        /**
         * Returns sampling percentage. Range is 0.0 to 1.0, exclusive
         */
        public float getSamplePercentage() {
            return samplePercentage;
        }


        /**
         * Indicates whether repeatable seed should be used.
         */
        public boolean isRepeatable() {
            return isRepeatable;
        }


        /**
         * Seed to produce repeatable samples.
         */
        public int getRepeatableSeed() {
            return repeatableSeed;
        }


        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append( isBernoulli ? "BERNOULLI" : "SYSTEM" );
            b.append( '(' );
            b.append( samplePercentage * 100.0 );
            b.append( ')' );

            if ( isRepeatable ) {
                b.append( " REPEATABLE(" );
                b.append( repeatableSeed );
                b.append( ')' );
            }
            return b.toString();
        }
    }
}
