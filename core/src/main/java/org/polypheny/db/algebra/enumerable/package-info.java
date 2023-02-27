
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

/**
 * Query optimizer rules for Java calling convention.
 */

<<<<<<<< HEAD:core/src/main/java/org/polypheny/db/algebra/enumerable/package-info.java
package org.polypheny.db.algebra.enumerable;
========
import java.util.Map;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Snapshot;

public class FullSnapshot implements Snapshot {


    public FullSnapshot( Map<Long, NCatalog> catalogs ) {

    }

>>>>>>>> afc600594 (boilerplate for interface of schemaSnapshot):core/src/main/java/org/polypheny/db/algebra/enumerable/FullSnapshot.java

