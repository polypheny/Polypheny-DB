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

package org.polypheny.db.nodes;

import java.util.List;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;

public interface Explain extends Node {

    List<Node> getOperandList();

    Node getExplicandum();

    ExplainLevel getDetailLevel();

    Depth getDepth();

    int getDynamicParamCount();

    boolean withType();

    ExplainFormat getFormat();

    /**
     * The level of abstraction with which to display the plan.
     */
    enum Depth {
        TYPE, LOGICAL, PHYSICAL;
    }

}
