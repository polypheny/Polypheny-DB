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
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JWindow;
import javax.swing.WindowConstants;
import javax.swing.border.EtchedBorder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplashHelper implements Runnable {

    private SplashScreen screen;


    @Override
    public void run() {
        this.screen = new SplashScreen();
    }


    public void setComplete() {
        screen.setComplete();
        try {
            Desktop.getDesktop().browse( new URL( "http://localhost:8080" ).toURI() );
        } catch ( IOException | URISyntaxException e ) {
            log.warn( "Polypheny was not able to open the browser for the user!" );
        }
    }


    public static class SplashScreen extends JWindow {


        private final JLabel text;
        private final JLabel picLabel;
        private final JFrame frame;


        public SplashScreen() {
            this.frame = new JFrame( "Polypheny" );
            frame.setIconImage( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "logo-600.png" ) ) ).getImage() );
            frame.setSize( 350, 250 );
            frame.setUndecorated( true );
            frame.setResizable( false );
            frame.setDefaultCloseOperation( WindowConstants.HIDE_ON_CLOSE );
            frame.getContentPane().setLayout( new BoxLayout( frame.getContentPane(), BoxLayout.Y_AXIS ) );

            JPanel panel = new JPanel( new BorderLayout() );
            panel.setBorder( new EtchedBorder() );
            frame.add( panel, BorderLayout.CENTER );

            JPanel textP = new JPanel();

            JLabel label = new JLabel( "Polypheny" );
            label.setFont( new Font( "Verdana", Font.BOLD, 16 ) );

            this.text = new JLabel( "is starting..." );
            text.setFont( new Font( "Verdana", Font.PLAIN, 14 ) );

            JLabel copy = new JLabel( String.format( "%s ©Polypheny 2021", getControlVersion() ) );
            copy.setFont( new Font( "Verdana", Font.PLAIN, 10 ) );

            this.picLabel = new JLabel( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "loading.gif" ) ) ) );

            textP.add( label );
            textP.add( text );

            panel.add( textP, BorderLayout.NORTH );
            panel.add( picLabel, BorderLayout.CENTER );
            panel.add( copy, BorderLayout.SOUTH );

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
            this.frame.setVisible( false );
            dispose();
        }

    }

}



