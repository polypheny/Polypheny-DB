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

package org.polypheny.db.webui.models;


import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;


public class PartitionFunctionModel {

    public String title;
    public String description;
    public List<String> columnNames;
    public List<List<PartitionFunctionColumn>> rows;

    @Setter
    public String functionName;
    public int numberOfPartitions;
    public long tableId;
    public long columnId;

    // Needed because requestJson in Crud.getPartitionFunctionModel() only delivers names instead of ids
    @Setter
    public String tableName;
    @Setter
    public String partitionColumnName;
    @Setter
    public String schemaName;

    public String error;


    public PartitionFunctionModel( final String title, final String description, final List<String> columnNames, final List<List<PartitionFunctionColumn>> rows ) {
        this.title = title;
        this.description = description;
        this.columnNames = columnNames;
        this.rows = rows;
    }


    public PartitionFunctionModel( final String error ) {
        this.error = error;
    }


    public enum FieldType {
        STRING, INTEGER, LIST, LABEL;
    }


    @Accessors(chain = true)
    public static class PartitionFunctionColumn {

        public FieldType type;
        @Setter
        public boolean mandatory = false;
        @Setter
        public boolean modifiable = true;
        public String value;
        public List<String> options;

        //Necessary altercation needed due to loss of context of original json
        @Setter
        public String sqlPrefix = "";
        @Setter
        public String sqlSuffix = "";


        public PartitionFunctionColumn( final FieldType type, final String defaultValue ) {
            this.type = type;
            this.value = defaultValue;
        }


        public PartitionFunctionColumn( final FieldType type, final List<String> options, final String defaultValue ) {
            this.type = type;
            this.value = defaultValue;
            this.options = options;
        }

    }

}
