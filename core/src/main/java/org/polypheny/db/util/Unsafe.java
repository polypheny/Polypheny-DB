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

package org.polypheny.db.util;


import java.io.StringWriter;


/**
 * Contains methods that call JDK methods that the <a href="https://github.com/policeman-tools/forbidden-apis">forbidden APIs checker</a>
 * does not approve of.
 * <p>
 * This class is excluded from the check, so methods called via this class will not fail the build.
 */
public class Unsafe {

    private Unsafe() {
    }


    /**
     * Calls {@link Object#wait()}.
     */
    public static void wait( Object o ) throws InterruptedException {
        o.wait();
    }


    /**
     * Clears the contents of a {@link StringWriter}.
     */
    public static void clear( StringWriter sw ) {
        // Included in this class because StringBuffer is banned.
        sw.getBuffer().setLength( 0 );
    }


    /**
     * Appends to {@link StringWriter}.
     */
    public static void append( StringWriter sw, CharSequence charSequence, int start, int end ) {
        // Included in this class because StringBuffer is banned.
        sw.getBuffer().append( charSequence, start, end );
    }

}
