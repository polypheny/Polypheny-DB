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

package org.polypheny.db.gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;
import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JWindow;
import javax.swing.WindowConstants;
import javax.swing.border.EtchedBorder;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatusService;


@Slf4j
public class SplashHelper implements Runnable {

    private SplashScreen screen;
    @Setter
    private int statusId;


    @Override
    public void run() {
        this.screen = new SplashScreen();
    }


    public void setComplete() {
        try {
            Desktop.getDesktop().browse( new URL( "http://localhost:8080" ).toURI() );
        } catch ( IOException | URISyntaxException e ) {
            log.warn( "Polypheny-DB was not able to open the browser for the user!" );
        }
        this.screen.setComplete();
        StatusService.removeSubscriber( statusId );
    }


    public void setStatus( String status ) {
        // the gui thread might not have set the screen yet, so we only print if the screen is up
        if ( screen != null ) {
            screen.setStatus( status );
        }
    }


    public static class SplashScreen extends JWindow {

        private final JLabel picLabel;
        private final JFrame frame;
        private final JLabel status;


        public SplashScreen() {
            this.frame = new JFrame( "Polypheny" );
            frame.setIconImage( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "logo-600.png" ) ) ).getImage() );
            frame.setSize( 400, 250 );
            frame.setUndecorated( true );
            frame.setResizable( false );
            frame.setDefaultCloseOperation( WindowConstants.HIDE_ON_CLOSE );
            frame.getContentPane().setLayout( new BoxLayout( frame.getContentPane(), BoxLayout.Y_AXIS ) );

            JPanel panel = new JPanel( new BorderLayout() );
            panel.setBorder( new EtchedBorder() );
            frame.add( panel, BorderLayout.CENTER );

            JPanel middle = new JPanel( new BorderLayout() );
            //middle.setBorder( BorderFactory.createEmptyBorder( 0, 12, 0, 12 ) );

            JLabel label = new JLabel( "Polypheny" );
            label.setFont( new Font( "Verdana", Font.BOLD, 24 ) );
            label.setBorder( BorderFactory.createEmptyBorder( 12, 12, 0, 12 ) );

            this.status = new JLabel( "loading..." );
            status.setFont( new Font( "Verdana", Font.PLAIN, 12 ) );

            JLabel version = new JLabel( getControlVersion() );
            version.setFont( new Font( "Verdana", Font.PLAIN, 10 ) );

            JLabel copy = new JLabel( "©2019-present The Polypheny Project" );
            copy.setFont( new Font( "Verdana", Font.PLAIN, 10 ) );

            JPanel bottom = new JPanel( new BorderLayout() );
            bottom.setBorder( BorderFactory.createEmptyBorder( 0, 12, 0, 12 ) );
            bottom.add( copy, BorderLayout.WEST );
            bottom.add( version, BorderLayout.EAST );

            this.picLabel = new JLabel( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "loading-32.gif" ) ) ) );
            picLabel.setBorder( BorderFactory.createEmptyBorder( 0, 12, 0, 12 ) );

            middle.add( picLabel, BorderLayout.WEST );
            middle.add( status );

            panel.add( label, BorderLayout.NORTH );
            panel.add( middle, BorderLayout.CENTER );
            panel.add( bottom, BorderLayout.SOUTH );

            Dimension dimension = Toolkit.getDefaultToolkit().getScreenSize();
            int x = (int) ((dimension.getWidth() - frame.getWidth()) / 2);
            int y = (int) ((dimension.getHeight() - frame.getHeight()) / 2);
            frame.setLocation( x, y );

            panel.setAlignmentX( Component.CENTER_ALIGNMENT );
            frame.setVisible( true );
        }


        public String getControlVersion() {
            String v = getClass().getPackage().getImplementationVersion();
            if ( v == null ) {
                return "Unknown";
            } else {
                return v;
            }
        }


        public void setComplete() {
            frame.setVisible( false );
            dispose();
        }


        public void setStatus( String status ) {
            this.status.setText( status );
        }

    }

}
