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

package org.polypheny.db.prisminterface.streaming;

import lombok.Getter;

@Getter
public class Estimate {

    private int dynamicLength;
    private int allStreamedLength;


    public Estimate() {
        this.dynamicLength = 0;
        this.allStreamedLength = 0;
    }


    public Estimate( int length ) {
        this.dynamicLength = length;
        this.allStreamedLength = length;
    }


    public Estimate addToAll( int length ) {
        this.dynamicLength += length;
        this.allStreamedLength += length;
        return this;
    }


    public void setDynamicLength( int dynamicLength ) {
        this.dynamicLength = dynamicLength;
    }


    public void setAllStreamedLength( int allStreamedLength ) {
        this.allStreamedLength = allStreamedLength;
    }


    public Estimate add( Estimate other ) {
        this.dynamicLength += other.dynamicLength;
        this.allStreamedLength += other.allStreamedLength;
        return this;
    }

}
