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


import org.polypheny.db.algebra.enumerable.AggImplementor;
import org.polypheny.db.algebra.enumerable.StrictAggImplementor;
import org.polypheny.db.algebra.enumerable.StrictWinAggImplementor;
import org.polypheny.db.algebra.enumerable.WinAggImplementor;


/**
 * Function that can be translated to java code.
 *
 * @see AggImplementor
 * @see WinAggImplementor
 * @see StrictAggImplementor
 * @see StrictWinAggImplementor
 */
public interface ImplementableAggFunction extends AggregateFunction {

    /**
     * Returns implementor that translates the function to linq4j expression.
     *
     * @param windowContext true when aggregate is used in window context
     * @return implementor that translates the function to linq4j expression.
     */
    AggImplementor getImplementor( boolean windowContext );

}
