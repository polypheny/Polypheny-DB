/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.backup;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;

import java.util.List;

public class InformationObject {

    //ImmutableMap<Long, LogicalNamespace> namespaces;
    @Getter @Setter
    List<LogicalNamespace> namespaces;
    @Getter @Setter
    List<LogicalNamespace> relNamespaces;
    @Getter @Setter
    List<LogicalNamespace> graphNamespaces;
    @Getter @Setter
    List<LogicalNamespace> docNamespaces;
    //TODO(FF): adjust
    @Getter @Setter
    List<LogicalView> views;
    @Getter @Setter
    List<LogicalMaterializedView> materializedViews;
    //namespace id, list of tables for the namespace
    @Getter @Setter
    ImmutableMap<Long, List<LogicalTable>> tables;

    @Getter @Setter
    ImmutableMap<Long, List<LogicalCollection>> collections;
    @Getter @Setter
    ImmutableMap<Long, LogicalGraph> graphs;

    //table id, list of views for the table
    ImmutableMap<Long, List<LogicalColumn>> columns;
    ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeysPerTable;
    ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable;
    //ImmutableMap<Long, List<LogicalKey>> keysPerTable;
    // uufspliite en pk, fk, constraints, indexes
    // index -> can only be created per (one) table
    ImmutableMap<Long, List<LogicalIndex>> logicalIndexes;
    //TODO(FF): if there exist constraint that go over several tables, need other way to signify it... rather use constraints per table, not per namespace! (but gets the right amount of constraints) --> constr only 1 table (?), views can be sever tables
    ImmutableMap<Long, List<LogicalConstraint>> constraints;

    Boolean collectedRel = false;
    Boolean collectedDoc = false;
    Boolean collectedGraph = false;
}
