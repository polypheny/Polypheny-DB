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

package org.polypheny.db.sql.language;


import java.util.Objects;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Litmus;


/**
 * A SQL literal representing a time interval.
 * <p>
 * Examples:
 *
 * <ul>
 * <li>INTERVAL '1' SECOND</li>
 * <li>INTERVAL '1:00:05.345' HOUR</li>
 * <li>INTERVAL '3:4' YEAR TO MONTH</li>
 * </ul>
 *
 * YEAR/MONTH intervals are not implemented yet.
 * <p>
 * The interval string, such as '1:00:05.345', is not parsed yet.
 */
public class SqlIntervalLiteral extends SqlLiteral {


    protected SqlIntervalLiteral( int sign, String intervalStr, SqlIntervalQualifier intervalQualifier, PolyType polyType, ParserPos pos ) {
        this( new IntervalValue( intervalQualifier, sign, intervalStr ), polyType, pos );
    }


    private SqlIntervalLiteral( IntervalValue intervalValue, PolyType polyType, ParserPos pos ) {
        super( intervalValue, polyType, pos );
    }


    @Override
    public SqlIntervalLiteral clone( ParserPos pos ) {
        return new SqlIntervalLiteral( (IntervalValue) value, getTypeName(), pos );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlIntervalLiteral( writer, this, leftPrec, rightPrec );
    }


    @Override
    @SuppressWarnings("deprecation")
    public int signum() {
        return ((IntervalValue) value).signum();
    }


    /**
     * A Interval value.
     */
    public static class IntervalValue extends PolyValue {

        @Getter
        private final SqlIntervalQualifier intervalQualifier;
        private final String intervalStr;
        @Getter
        private final int sign;


        /**
         * Creates an interval value.
         *
         * @param intervalQualifier Interval qualifier
         * @param sign Sign (+1 or -1)
         * @param intervalStr Interval string
         */
        IntervalValue( SqlIntervalQualifier intervalQualifier, int sign, String intervalStr ) {
            super( null );
            assert (sign == -1) || (sign == 1);
            assert intervalQualifier != null;
            assert intervalStr != null;
            this.intervalQualifier = intervalQualifier;
            this.sign = sign;
            this.intervalStr = intervalStr;
        }


        public boolean equals( Object obj ) {
            if ( !(obj instanceof IntervalValue that) ) {
                return false;
            }
            return this.intervalStr.equals( that.intervalStr )
                    && (this.sign == that.sign)
                    && this.intervalQualifier.equalsDeep( that.intervalQualifier, Litmus.IGNORE );
        }


        public int hashCode() {
            return Objects.hash( sign, intervalStr, intervalQualifier );
        }


        public String getIntervalLiteral() {
            return intervalStr;
        }


        public int signum() {
            for ( int i = 0; i < intervalStr.length(); i++ ) {
                char ch = intervalStr.charAt( i );
                if ( ch >= '1' && ch <= '9' ) {
                    // If non zero return sign.
                    return getSign();
                }
            }
            return 0;
        }


        public String toString() {
            return intervalStr;
        }


        @Override
        public int compareTo( @NotNull PolyValue o ) {
            throw new GenericRuntimeException( "Not allowed" );
        }


        @Override
        public Expression asExpression() {
            throw new GenericRuntimeException( "Not allowed" );
        }


        @Override
        public PolySerializable copy() {
            throw new GenericRuntimeException( "Not allowed" );
        }


        @Override
        public @Nullable Long deriveByteSize() {
            return null;
        }


        @Override
        public Object toJava() {
            return this;
        }

    }

}
