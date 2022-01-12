/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema;


import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.util.Pair;


/**
 * Extension to {@link Table} that specifies a custom way to resolve column names.
 *
 * It is optional for a Table to implement this interface. If Table does not implement this interface, column resolving will
 * be performed in the default way.
 *
 * <strong>NOTE: This class is experimental and subject to change/removal without notice</strong>.
 */
public interface CustomColumnResolvingTable extends Table {

    /**
     * Resolve a column based on the name components. One or more the input name components can be resolved to one field in
     * the table row type, along with a remainder list of name components which have not been resolved within this call, and
     * which in turn can be potentially resolved as sub-field names. In the meantime, this method can return multiple matches,
     * which is a list of pairs containing the resolved field and the remaining name components.
     *
     * @param rowType the table row type
     * @param typeFactory the type factory
     * @param names the name components to be resolved
     * @return a list of pairs containing the resolved field and the remaining name components.
     */
    List<Pair<AlgDataTypeField, List<String>>> resolveColumn(
            AlgDataType rowType,
            AlgDataTypeFactory typeFactory,
            List<String> names );

}

