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

package org.polypheny.db;

import java.awt.Desktop;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Status service is a utility class, which allows sending different types of statuses
 * to the attached subscribers.
 */
@Slf4j
public class StatusService {

    private static final String POLY_URL = "http://localhost:8080";

    private static final AtomicInteger idBuilder = new AtomicInteger();

    private static final Map<Integer, BiConsumer<String, Object>> infoSubs = new HashMap<>();
    private static final Map<Integer, BiConsumer<String, Object>> errorSubs = new HashMap<>();


    public static synchronized int addInfoSubscriber( BiConsumer<String, Object> printer ) {
        return StatusService.addSubscriber( printer, StatusType.INFO );
    }


    public static synchronized int addSubscriber( BiConsumer<String, Object> printer, StatusType type ) {
        int id = idBuilder.getAndIncrement();
        if ( type == StatusType.INFO ) {
            infoSubs.put( id, printer );
        } else {
            errorSubs.put( id, printer );
        }

        return id;
    }


    public static void printInfo( String status, Object arg ) {
        fireOnSubs( infoSubs, status, arg );
    }


    public static void printInfo( String status ) {
        printInfo( status, null );
    }


    public static void printError( String status, ErrorConfig config ) {
        fireOnSubs( errorSubs, status, config );
    }


    private static void fireOnSubs( Map<Integer, BiConsumer<String, Object>> subs, String status, Object arg ) {
        subs.values().forEach( c -> c.accept( status, arg ) );
    }


    public static void removeSubscriber( int id ) {
        infoSubs.remove( id );
        errorSubs.remove( id );
    }


    public enum StatusType {
        INFO,
        ERROR
    }


    /**
     * Config class, which allows to configure the behavior of errors printed
     */
    @Builder
    @Getter
    @Accessors(fluent = true)
    public static class ErrorConfig {

        // Default method to open Polypheny in the default browser
        public static Consumer<?> OPEN_BROWSER = e -> {
            try {
                Desktop.getDesktop().browse( new URL( POLY_URL ).toURI() );
            } catch ( IOException | URISyntaxException ex ) {
                log.warn( "Polypheny-DB was not able to open the browser for the user!" );
            }
        };

        // Utility method, which does nothing
        public static Consumer<?> DO_NOTHING = e -> {
            // empty on purpose
        };

        @Default
        String buttonMessage = "OK";
        @Default
        boolean doExit = false;
        @Default
        boolean doBlock = true;
        @Default
        boolean showButton = true;
        @Default
        Consumer<?> func = OPEN_BROWSER;

    }

}
