/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.schemaDiscovery;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface MetadataProvider {

    AbstractNode fetchMetadataTree();

    List<Map<String, Object>> fetchPreview( Connection conn, String fqName, int limit );

    void markSelectedAttributes( List<String> selectedPaths );

    void printTree( AbstractNode node, int depth );

    void setRoot( AbstractNode root );

    Object getPreview();


}
