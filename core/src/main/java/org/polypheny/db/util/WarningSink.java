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

package org.polypheny.db.util;

import java.util.function.Consumer;

/**
 * Adds GUI-visible warnings without depending on a specific Statement/collector API.
 * Tries common variants via reflection and degrades to no-op if none match.
 */
public final class WarningSink {

    private final Consumer<String> delegate;

    private WarningSink(Consumer<String> delegate) {
        this.delegate = delegate != null ? delegate : s -> {};
    }

    /** Create a WarningSink from a Statement or execution context object. */
    public static WarningSink from(Object statement) {
        if (statement == null) return new WarningSink(null);

        // 1) statement.getWarningCollector().add(String)
        try {
            Object wc = statement.getClass().getMethod("getWarningCollector").invoke(statement);
            if (wc != null) {
                try {
                    var add = wc.getClass().getMethod("add", String.class);
                    return new WarningSink(msg -> {
                        try { add.invoke(wc, msg); } catch (Throwable ignored) {}
                    });
                } catch (Throwable ignored) {}

                // 1b) collector.add(Warning.of(String))
                try {
                    Class<?> W = Class.forName("org.polypheny.db.util.Warning");
                    var of = W.getMethod("of", String.class);
                    var addW = wc.getClass().getMethod("add", W);
                    return new WarningSink(msg -> {
                        try { addW.invoke(wc, of.invoke(null, msg)); } catch (Throwable ignored) {}
                    });
                } catch (Throwable ignored) {}
            }
        } catch (Throwable ignored) {}

        // 2) statement.addWarning(String)
        try {
            var add = statement.getClass().getMethod("addWarning", String.class);
            return new WarningSink(msg -> {
                try { add.invoke(statement, msg); } catch (Throwable ignored) {}
            });
        } catch (Throwable ignored) {}

        return new WarningSink(null);
    }

    /** Add one warning (visible in the client if the Statement supports it). */
    public void add(String message) {
        delegate.accept(message);
    }
}

