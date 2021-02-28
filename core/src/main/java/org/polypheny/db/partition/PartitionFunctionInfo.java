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
 */


package org.polypheny.db.partition;


import java.util.List;
import lombok.Builder;
import lombok.Getter;


@Getter
@Builder
public class PartitionFunctionInfo {

    private String functionTitle;
    private String description;
    private String sqlPrefix;
    private String sqlSuffix;
    private String rowSeparation;
    private List<String> headings;


    public enum PartitionFunctionInfoColumnType {STRING, INTEGER, LIST, LABEL}


    //rows_before List of List of Inner Class
    private List<List<PartitionFunctionInfoColumn>> rowsBefore;

    //dynamic_rows Which will be defined ones and then repeated as often as 'n' (number of Partitions is specified)
    private List<PartitionFunctionInfoColumn> dynamicRows;

    //rows_after List of List of Inner Class
    private List<List<PartitionFunctionInfoColumn>> rowsAfter;


    @Getter
    @Builder
    public static class PartitionFunctionInfoColumn {

        private PartitionFunctionInfoColumnType fieldType;
        private boolean mandatory;
        private boolean modifiable;
        private String sqlPrefix;
        private String sqlSuffix;
        private String defaultValue;
        private String valueSeparation;
        private List<String> options;

    }

}
