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

package org.polypheny.db.util;


import java.util.List;
import org.polypheny.db.runtime.ConsList;
import org.polypheny.db.runtime.PolyphenyDbResource;
import org.polypheny.db.runtime.Resources;


/**
 * Definitions of objects to be statically imported.
 *
 * <h3>Note to developers</h3>
 *
 * Please give careful consideration before including an object in this class. Pros:
 * <ul>
 * <li>Code that uses these objects will be terser.</li>
 * </ul>
 *
 * Cons:
 * <ul>
 * <li>Namespace pollution,</li>
 * <li>code that is difficult to understand (a general problem with static imports),</li>
 * <li>potential cyclic initialization.</li>
 * </ul>
 */
public abstract class Static {

    private Static() {
    }


    /**
     * Resources.
     */
    public static final PolyphenyDbResource RESOURCE = Resources.create( PolyphenyDbResource.class );


    /**
     * Builds a list.
     */
    public static <E> List<E> cons( E first, List<? extends E> rest ) {
        return ConsList.of( first, rest );
    }
}

