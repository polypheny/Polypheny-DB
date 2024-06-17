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

package org.polypheny.db.prepare;


import java.util.List;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Statement;


/**
 * Context for preparing a statement.
 */
public interface Context {

    JavaTypeFactory getTypeFactory();

    /**
     * Returns the associated snapshot.
     */
    Snapshot getSnapshot();

    String getDefaultNamespaceName();

    void updateSnapshot();

    DataContext getDataContext();

    /**
     * Returns the path of the object being analyzed, or null.
     * <p>
     * The object is being analyzed is typically a view. If it is already being analyzed further up the stack,
     * the view definition can be deduced to be cyclic.
     */
    List<String> getObjectPath();

    Statement getStatement();

}
