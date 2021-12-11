/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.plan;


/**
 * AlgOptCostImpl provides a default implementation for the {@link AlgOptCost} interface. It it defined in terms of a single scalar quantity; somewhat
 * arbitrarily, it returns this scalar for rows processed and zero for both CPU and I/O.
 */
public class AlgOptCostImpl implements AlgOptCost {

    public static final AlgOptCostFactory FACTORY = new Factory();


    private final double value;


    public AlgOptCostImpl( double value ) {
        this.value = value;
    }


    // implement RelOptCost
    @Override
    public double getRows() {
        return value;
    }


    // implement RelOptCost
    @Override
    public double getIo() {
        return 0;
    }


    // implement RelOptCost
    @Override
    public double getCpu() {
        return 0;
    }


    // implement RelOptCost
    @Override
    public boolean isInfinite() {
        return Double.isInfinite( value );
    }


    @Override
    public double getCosts() {
        return getRows() + getCpu() + getIo();
    }


    // implement RelOptCost
    @Override
    public boolean isLe( AlgOptCost other ) {
        return getRows() <= other.getRows();
    }


    // implement RelOptCost
    @Override
    public boolean isLt( AlgOptCost other ) {
        return getRows() < other.getRows();
    }


    @Override
    public int hashCode() {
        return Double.hashCode( getRows() );
    }


    // implement RelOptCost
    @Override
    public boolean equals( AlgOptCost other ) {
        return getRows() == other.getRows();
    }


    @Override
    public boolean equals( Object obj ) {
        if ( obj instanceof AlgOptCostImpl ) {
            return equals( (AlgOptCost) obj );
        }
        return false;
    }


    // implement RelOptCost
    @Override
    public boolean isEqWithEpsilon( AlgOptCost other ) {
        return Math.abs( getRows() - other.getRows() ) < AlgOptUtil.EPSILON;
    }


    // implement RelOptCost
    @Override
    public AlgOptCost minus( AlgOptCost other ) {
        return new AlgOptCostImpl( getRows() - other.getRows() );
    }


    // implement RelOptCost
    @Override
    public AlgOptCost plus( AlgOptCost other ) {
        return new AlgOptCostImpl( getRows() + other.getRows() );
    }


    // implement RelOptCost
    @Override
    public AlgOptCost multiplyBy( double factor ) {
        return new AlgOptCostImpl( getRows() * factor );
    }


    @Override
    public double divideBy( AlgOptCost cost ) {
        AlgOptCostImpl that = (AlgOptCostImpl) cost;
        return this.getRows() / that.getRows();
    }


    // implement RelOptCost
    public String toString() {
        if ( value == Double.MAX_VALUE ) {
            return "huge";
        } else {
            return Double.toString( value );
        }
    }


    /**
     * Implementation of {@link AlgOptCostFactory} that creates {@link AlgOptCostImpl}s.
     */
    private static class Factory implements AlgOptCostFactory {

        // implement RelOptPlanner
        @Override
        public AlgOptCost makeCost( double dRows, double dCpu, double dIo ) {
            return new AlgOptCostImpl( dRows );
        }


        // implement RelOptPlanner
        @Override
        public AlgOptCost makeHugeCost() {
            return new AlgOptCostImpl( Double.MAX_VALUE );
        }


        // implement RelOptPlanner
        @Override
        public AlgOptCost makeInfiniteCost() {
            return new AlgOptCostImpl( Double.POSITIVE_INFINITY );
        }


        // implement RelOptPlanner
        @Override
        public AlgOptCost makeTinyCost() {
            return new AlgOptCostImpl( 1.0 );
        }


        // implement RelOptPlanner
        @Override
        public AlgOptCost makeZeroCost() {
            return new AlgOptCostImpl( 0.0 );
        }

    }

}
