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

package org.polypheny.db.webui.models;


import com.fasterxml.jackson.annotation.JsonProperty;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;


/**
 * Model for a {@link AlgNode} coming from the Alg-Builder in the UI
 */
public class UIAlgNode {

    @JsonProperty("class")
    public String clazz;

    /**
     * ExpressionType of the AlgNode, e.g. Scan
     */
    @JsonProperty
    public String type;

    /**
     * ExpressionType of Table, e.g. Table, View
     */
    @JsonProperty
    public String entityType;

    //tableScan
    @JsonProperty
    public String entityName;

    /**
     * Children of this node in the tree
     */
    @JsonProperty
    public UIAlgNode[] children;

    /**
     * Number of inputs of a node.
     * Required by the AlgBuilder
     */
    @JsonProperty
    public int inputCount;



    //join
    @JsonProperty
    public JoinAlgType join;
    //join condition
    @JsonProperty
    public String operator;
    @JsonProperty
    public String col1;
    @JsonProperty
    public String col2;

    //filter
    //(String operator)
    @JsonProperty
    public String field;
    @JsonProperty
    public String filter;

    //project
    @JsonProperty
    public String[] fields;

    //aggregate
    @JsonProperty
    public String groupBy;
    @JsonProperty
    public String aggregation;
    @JsonProperty
    public String alias;
    //(String field)

    //sort
    @JsonProperty
    public SortState[] sortColumns;

    //union, minus
    public boolean all;

}
