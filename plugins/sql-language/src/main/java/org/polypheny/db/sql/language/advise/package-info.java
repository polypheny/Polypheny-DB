
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
 * Provides hints and corrections for editing SQL statements.
 *
 * The SQL statement might be partially-formed SQL statement or invalid. It is edited in a SQL editor user-interface.
 *
 * The advisor uses the validation and parser framework set up in <code>org.polypheny.db.sql.validate</code> package.
 */

package org.polypheny.db.sql.language.advise;

