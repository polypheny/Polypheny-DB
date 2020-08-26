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
 */

package org.polypheny.db.config;


import java.util.Arrays;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import org.polypheny.db.webui.ConfigServer;


public class ConfigServerTest {


    public static void main( String[] args ) {
        ConfigServer s = new ConfigServer( 8081 );
        demoData( s );
    }


    /**
     * Test data
     */
    private static void demoData( ConfigServer s ) {
        System.out.println( "demoData()" );

        ConfigManager cm = ConfigManager.getInstance();

        //todo page with icon
        WebUiPage p1 = new WebUiPage( "p1", "php.ini (1)", "Configuration for MAMP (php.ini), part 1." );
        WebUiPage p2 = new WebUiPage( "p2", "php.ini (2)", "Configuration for MAMP (php.ini), part 2." );

        WebUiGroup g1 = new WebUiGroup( "g1", "p1" ).withTitle( "Language Options" ).withDescription( "These are the settings for the php language." );
        WebUiGroup g2 = new WebUiGroup( "g2", "p1" ).withTitle( "Miscellaneous" ).withDescription( "Miscellaneous options" );
        WebUiGroup g3 = new WebUiGroup( "g3", "p1" ).withTitle( "Resource Limits" ).withDescription( "Set the resource limits for php" );
        WebUiGroup g4 = new WebUiGroup( "g4", "p1" ).withTitle( "Error handling and logging" ).withDescription( "Settings for the error handling and logging" );
        WebUiGroup g5 = new WebUiGroup( "g5", "p2" ).withTitle( "Data handling" );
        WebUiGroup g6 = new WebUiGroup( "g6", "p2" ).withTitle( "Paths and Directories" );
        WebUiGroup g7 = new WebUiGroup( "g7", "p2" ).withTitle( "File uploads" );
        WebUiGroup g8 = new WebUiGroup( "g8", "p2" ).withTitle( "Fopen wrappers" );

        cm.registerWebUiPage( p1 );
        cm.registerWebUiPage( p2 );
        cm.registerWebUiGroup( g1 );
        cm.registerWebUiGroup( g2 );
        cm.registerWebUiGroup( g3 );
        cm.registerWebUiGroup( g4 );
        cm.registerWebUiGroup( g5 );
        cm.registerWebUiGroup( g6 );
        cm.registerWebUiGroup( g7 );
        cm.registerWebUiGroup( g8 );

        Config c1 = new ConfigBoolean( "engine", true ).withUi( "g1" );
        Config c2 = new ConfigBoolean( "short_open_tag", false ).withUi( "g1" );
        Config c3 = new ConfigInteger( "precision", 14 ).withUi( "g1" );
        Config c4 = new ConfigInteger( "output_buffering", 4096 ).withUi( "g1" ).withWebUiValidation( WebUiValidator.REQUIRED );
        Config c5 = new ConfigBoolean( "zlib.output_compression", false ).withUi( "g1" );
        Config c6 = new ConfigBoolean( "implicit_flush", false ).withUi( "g1" );
        Config c7 = new ConfigInteger( "serialize_precision", -1 ).withUi( "g1" );
        Config c8 = new ConfigBoolean( "zend.enable_gc", true ).withUi( "g1" );
        cm.registerConfigs( c1, c2, c3, c4, c5, c6, c7, c8 );

        Config c9 = new ConfigBoolean( "expose_php", true ).withUi( "g2", 1 );
        cm.registerConfig( c9 );

        Config clazz1 = new ConfigClazz( "clazz1", TestClass.class, FooImplementation.class ).withUi( "g2", 2 );
        cm.registerConfig( clazz1 );

        Config clazzList = new ConfigClazzList( "clazz_list", TestClass.class, Arrays.asList( FooImplementation.class, BarImplementation.class ) ).withUi( "g2", 3 );
        cm.registerConfig( clazzList );

        Config enum1 = new ConfigEnum( "enum", "Test description", TestEnum.class, TestEnum.A ).withUi( "g2", 4 );
        cm.registerConfig( enum1 );

        Config enumList = new ConfigEnumList( "enumList", "Test description", TestEnum.class, Arrays.asList( TestEnum.A, TestEnum.B ) ).withUi( "g2", 5 );
        cm.registerConfig( enumList );

        Config c10 = new ConfigInteger( "max_execution_time", 30 ).withUi( "g3" );
        Config c11 = new ConfigInteger( "max_input_time", 60 ).withUi( "g3" );
        Config c12 = new ConfigString( "memory_limit", "128M" ).withUi( "g3" );
        cm.registerConfigs( c10, c11, c12 );

        Config c13 = new ConfigString( "error_reporting", "E_ALL" ).withUi( "g4" );
        Config c14 = new ConfigBoolean( "display_errors", false ).withUi( "g4" );
        Config c15 = new ConfigBoolean( "display_startup_errors", false ).withUi( "g4" );
        Config c16 = new ConfigBoolean( "log_errors", true ).withUi( "g4" );
        Config c17 = new ConfigInteger( "log_errors_max_len", 1024 ).withUi( "g4" );
        Config c18 = new ConfigBoolean( "ignore_repeated_errors", false ).withUi( "g4" );
        Config c19 = new ConfigBoolean( "ignore_repeated_source", false ).withUi( "g4" );
        Config c20 = new ConfigBoolean( "report_memleaks", true ).withUi( "g4" );
        Config c21 = new ConfigBoolean( "html_errors", true ).withUi( "g4" );
        Config c22 = new ConfigString( "error_log", "/Applications/MAMP/logs/php_error.log" ).withUi( "g4" );
        cm.registerConfigs( c13, c14, c15, c16, c17, c18, c19, c20, c21, c22 );

        Config c23 = new ConfigString( "request_order", "GP" ).withUi( "g5" );
        Config c24 = new ConfigBoolean( "register_argc_argv", true ).withUi( "g5" );
        Config c25 = new ConfigBoolean( "auto_globals_jit", true ).withUi( "g5" );
        Config c26 = new ConfigString( "post_max_size", "8M" ).withUi( "g5" );
        Config c27 = new ConfigString( "default_mimetype", "text/html" ).withUi( "g5" );
        Config c28 = new ConfigString( "default_charset", "UTF-8" ).withUi( "g5" );
        cm.registerConfigs( c23, c24, c25, c26, c27, c28 );

        Config c29 = new ConfigString( "include_path", ".:/Applications/MAMP/bin/php/php7.2.10/lib/php" ).withUi( "g6" );
        Config c30 = new ConfigString( "extension_dir", "Applications/MAMP/bin/php/php7.2.10/lib/php/extensions/no-debug-non-zts-20170718/" ).withUi( "g6" );
        Config c31 = new ConfigBoolean( "enable_dl", true ).withUi( "g6" );
        cm.registerConfigs( c29, c30, c31 );

        Config c32 = new ConfigBoolean( "file_uploads", true ).withUi( "g7" );
        Config c33 = new ConfigString( "upload_tmp_dir", "/Applications/MAMP/tmp/php" ).withUi( "g7" );
        Config c34 = new ConfigString( "upload_max_filesize", "32M" ).withUi( "g7" );
        Config c35 = new ConfigInteger( "max_file_uploads", 20 ).withUi( "g7" ).withJavaValidation( a -> (int) a > 0 );
        cm.registerConfigs( c32, c33, c34, c35 );

        Config c36 = new ConfigBoolean( "allow_url_fopen", true ).withUi( "g8" );
        Config c37 = new ConfigBoolean( "allow_url_include", false ).withUi( "g8" );
        Config c38 = new ConfigInteger( "default_socket_timeout", 60 ).withUi( "g8" );
        cm.registerConfigs( c36, c37, c38 );

        cm.observeAll( s );

        //timer for UI testing
        Timer timer = new Timer();
        timer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                Random r = new Random();
                cm.getConfig( "precision" ).setInt( r.nextInt( 100 ) );
            }
        }, 10000, 10000 );

    }

    private static class TestClass {
        int a;
    }
    private static class FooImplementation extends TestClass {
        int b;
    }
    private static class BarImplementation extends TestClass {
        int c;
    }
    private static class FooBarImplementation extends TestClass {
        int d;
    }

    private enum TestEnum {
        A, B, C
    }


}
