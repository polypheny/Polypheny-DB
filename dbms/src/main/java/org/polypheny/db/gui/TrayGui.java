/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.gui;

import java.awt.AWTException;
import java.awt.EventQueue;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.Taskbar;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TrayGui {

    private static TrayGui INSTANCE;
    private TrayIcon trayIcon;


    public static TrayGui getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new TrayGui();
        }
        return INSTANCE;
    }


    private TrayGui() {
        // Checking for support
        if ( !SystemTray.isSupported() ) {
            log.error( "System tray is not supported! Use headless mode instead." );
            System.exit( 1 );
        }

        // Get the systemTray of the system
        SystemTray systemTray = SystemTray.getSystemTray();

        // Popup menu
        PopupMenu trayPopupMenu = new PopupMenu();

        // Setting tray icon
        ClassLoader classLoader = this.getClass().getClassLoader();
        Image icon = Toolkit.getDefaultToolkit().getImage( classLoader.getResource( "icon.png" ) );
        trayIcon = new TrayIcon( icon, "Polypheny", trayPopupMenu );
        trayIcon.setImageAutoSize( true ); // Adjust to default size as per system recommendation

        // Set application icon (displayed in some os in notifications)
        try {
            Taskbar taskbar = Taskbar.getTaskbar();
            // Set icon for macOS (and other systems which do support this method)
            taskbar.setIconImage( icon );
        } catch ( UnsupportedOperationException e ) {
            log.error( "The OS does not support: 'taskbar.setIconImage'", e );
        } catch ( SecurityException e ) {
            log.error( "There was a security exception for: 'taskbar.setIconImage'", e );
        }

        // Show about window
        MenuItem aboutItem = new MenuItem( "About" );
        aboutItem.addActionListener( e -> {
            EventQueue.invokeLater( () -> {
                try {
                    AboutWindow window = new AboutWindow();
                    window.setVisible();
                } catch ( Exception ex ) {
                    log.error( "Caught exception in about window", ex );
                }
            } );
        } );
        trayPopupMenu.add( aboutItem );

        // Open Polypheny-UI in the browser
        MenuItem puiItem = new MenuItem( "User Interface" );
        puiItem.addActionListener( e -> {
            GuiUtils.openUiInBrowser();
        } );
        trayPopupMenu.add( puiItem );

        // Add separator
        trayPopupMenu.addSeparator();

        // Quit option
        MenuItem shutdownItem = new MenuItem( "Shutdown" );
        shutdownItem.addActionListener( e -> {
            systemTray.remove( trayIcon );
            System.exit( 0 );
        } );
        trayPopupMenu.add( shutdownItem );

        // Add to the system tray
        try {
            systemTray.add( trayIcon );
        } catch ( AWTException awtException ) {
            awtException.printStackTrace();
        }

        // Initialize notification manager
        NotificationManager.setTrayIcon( trayIcon );

        // Output message to the user
        NotificationManager.info( "has been added to your system tray." );
    }


    public void shutdown() {
        SystemTray.getSystemTray().remove( trayIcon );
    }

}
