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

package org.polypheny.db.polyfier.core.construct.nodes;


import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum OperatorType {
    UNION("Union", 2),
    MINUS("Minus", 2),
    INTERSECT("Intersect", 2),
    JOIN("Join", 2),
    AGGREGATE("Aggregate", 2),
    FILTER("Filter", 1),
    PROJECT("Project", 1),
    SORT("Sort", 1),
    SCAN("TableScan", 0);

    final String name;
    final int inputOrdinal;

    public static final List<OperatorType> OPERATOR_TYPES = List.of(
            UNION, MINUS, INTERSECT, JOIN, AGGREGATE, FILTER, PROJECT, SORT
    );

    public static final List<OperatorType> SET_OPERATOR_TYPES = List.of(
            UNION, INTERSECT, MINUS
    );

    public static final List<OperatorType> JOIN_OPERATOR_TYPES = List.of(
            JOIN
    );

}
