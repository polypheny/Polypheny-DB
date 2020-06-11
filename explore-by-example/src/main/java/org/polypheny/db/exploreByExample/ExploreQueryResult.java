/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.exploreByExample;


import java.util.List;
import lombok.Getter;


public class ExploreQueryResult {

    @Getter
    public String[][] data;
    public String col;
    public int count;
    List<String> typeInfo;
    List<String> name;


    public ExploreQueryResult() {

    }


    public ExploreQueryResult( String[][] data, int count, List<String> colInfo, List<String> name ) {
        this.data = data;
        this.count = count;
        this.typeInfo = colInfo;
        this.name = name;
    }


    public ExploreQueryResult( String col, int count, List<String> colInfo, List<String> name ) {
        this.col = col;
        this.count = count;
        this.typeInfo = colInfo;
        this.name = name;
    }

}
