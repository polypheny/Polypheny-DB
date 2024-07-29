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

package org.polypheny.db.webui;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigClazz;
import org.polypheny.db.config.ConfigClazzList;
import org.polypheny.db.config.ConfigDecimal;
import org.polypheny.db.config.ConfigDouble;
import org.polypheny.db.config.ConfigEnum;
import org.polypheny.db.config.ConfigEnumList;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.config.ConfigLong;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.config.WebUiPage;


public class UiTestingConfigPage {


    private static class TestClass {

        int a;

    }


    private enum testEnum {FOO, BAR, FOO_BAR}


    private static class FooImplementation extends TestClass {

        int b;

    }


    private static class BarImplementation extends TestClass {

        int c;

    }


    static {
        ConfigManager cm = ConfigManager.getInstance();

        WebUiPage p1 = new WebUiPage( "uiconfigtest", "UiConfigTestPage", "Configuration Description" );

        WebUiGroup g1 = new WebUiGroup( "g1", p1.getId(), 1 ).withTitle( "Config" ).withDescription( "Select Config Options" );
        WebUiGroup g2 = new WebUiGroup( "g2", p1.getId() ).withTitle( "Config Too" ).withDescription( "Select Config Options" );

        cm.registerWebUiPage( p1 );
        cm.registerWebUiGroup( g1 );
        cm.registerWebUiGroup( g2 );

        Config c1 = new ConfigBoolean( "Boolean True Test", true ).withUi( "g1" );
        Config c2 = new ConfigBoolean( "Boolean False Test", false ).withUi( "g1" );
        Config c3 = new ConfigInteger( "Integer Test", 11 ).withUi( "g1" );
        Config c4 = new ConfigInteger( "Negative Integer Test", -1 ).withUi( "g1" );
        Config c5 = new ConfigClazz( "clazz Test", TestClass.class, FooImplementation.class ).withUi( "g1" );

        List<Class> l = new ArrayList<>();
        l.add( FooImplementation.class );
        l.add( BarImplementation.class );

        Config c6 = new ConfigEnumList( "enumList", "Test description", testEnum.class, ImmutableList.of( testEnum.BAR ) ).withUi( "g1" );
        Config c7 = new ConfigClazzList( "clazzList Test", TestClass.class, l ).withUi( "g2" );
        Config c8 = new ConfigDecimal( "Decimal Test", BigDecimal.valueOf( 43.43431 ) ).withUi( "g2" );
        Config c9 = new ConfigDouble( "Double Test", Double.valueOf( 2.2 ) ).withUi( "g2" );
        Config c10 = new ConfigEnum( "Enum Test", "Test description", testEnum.class, testEnum.FOO_BAR ).withUi( "g2" );
        List<Integer> list = new ArrayList<Integer>();
        list.add( 1 );
        list.add( 2 );
        list.add( 3 );

        Config c11 = new ConfigLong( "Long", 12312L ).withUi( "g2" );

        // Config c45 = new ConfigList("List",list,TestClass.class);

//        int[] array = { 1, 2, 3, 4, 5 };
//        Config c40 = new ConfigArray( "array", array ).withUi("g1");

//        int[][] table = new int[][]{
//                { 1, 2, 3 },
//                { 4, 5, 6 }
//        };
//        Config c41 = new ConfigTable( "table", table ).withUi("g1");
        cm.registerConfigs( c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11 );

    }


}
