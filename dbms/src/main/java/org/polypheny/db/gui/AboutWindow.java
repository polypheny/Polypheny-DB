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

import java.awt.Color;
import java.awt.Desktop;
import java.awt.Font;
import java.net.URI;
import java.util.Objects;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextPane;
import javax.swing.SwingConstants;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AboutWindow {

    private JFrame frame;


    public AboutWindow() {
        initialize();
    }


    private void initialize() {
        frame = new JFrame();
        frame.setIconImage( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "logo-600.png" ) ) ).getImage() );
        frame.setResizable( false );
        frame.setTitle( "Polypheny" );
        frame.setBounds( 100, 100, 505, 300 );
        frame.setDefaultCloseOperation( JFrame.HIDE_ON_CLOSE );
        frame.add( new AboutPanel() );
        frame.setAlwaysOnTop( true );
    }


    public void setVisible() {
        frame.setVisible( true );
    }


    private static class AboutPanel extends JPanel {

        @SneakyThrows
        public AboutPanel() {
            setBounds( 100, 100, 505, 300 );
            setLayout( null );

            JLabel lblIcon = new JLabel( new ImageIcon( Objects.requireNonNull( getClass().getClassLoader().getResource( "logo-landscape-80.png" ) ) ) );
            lblIcon.setBounds( 20, 20, 260, 80 );
            add( lblIcon );

            JTextPane txtpnVersion = new JTextPane();
            txtpnVersion.setText( "Version " + GuiUtils.getPolyphenyVersion() + ", built on " + GuiUtils.getBuildDate() );
            txtpnVersion.setFont( new Font( "Verdana", Font.PLAIN, 14 ) );
            txtpnVersion.setBackground( lblIcon.getBackground() );
            txtpnVersion.setEditable( false );
            txtpnVersion.setBounds( 100, 100, 400, 20 );
            add( txtpnVersion );

            JButton ossButton = new JButton();
            final URI ossUri = new URI( "https://polypheny.org/community/acknowledgements/acknowledgements.txt" );
            ossButton.setText( "open-source software" );
            ossButton.setBorderPainted( false );
            ossButton.setOpaque( false );
            ossButton.setFont( new Font( "Verdana", Font.PLAIN, 14 ) );
            ossButton.setForeground( Color.blue );
            ossButton.setBackground( lblIcon.getBackground() );
            ossButton.setToolTipText( "Acknowledgements" );
            ossButton.addActionListener( e -> openWebsite( ossUri ) );
            ossButton.setBounds( 258, 153, 240, 20 );
            ossButton.setHorizontalAlignment( SwingConstants.LEFT );
            add( ossButton );

            JTextPane txtpnCopyright = new JTextPane();
            txtpnCopyright.setText( "Copyright 2019-2022 The Polypheny Project\n"
                    + "Polypheny is powered by " );
            txtpnCopyright.setFont( new Font( "Verdana", Font.PLAIN, 14 ) );
            txtpnCopyright.setBackground( lblIcon.getBackground() );
            txtpnCopyright.setEditable( false );
            txtpnCopyright.setBounds( 100, 135, 400, 40 );
            add( txtpnCopyright );

            JTextPane txtpnRuntime = new JTextPane();
            txtpnRuntime.setText( "Runtime version: " + System.getProperty( "java.runtime.version" ) + "\n"
                    + "VM: " + System.getProperty( "java.vm.name" ) );
            txtpnRuntime.setFont( new Font( "Verdana", Font.PLAIN, 14 ) );
            txtpnRuntime.setBackground( lblIcon.getBackground() );
            txtpnRuntime.setEditable( false );
            txtpnRuntime.setBounds( 100, 185, 400, 40 );
            add( txtpnRuntime );

            JButton websiteButton = new JButton();
            final URI websiteUri = new URI( "https://polypheny.org/" );
            websiteButton.setText( "https://polypheny.org" );
            websiteButton.setBorderPainted( false );
            websiteButton.setOpaque( false );
            websiteButton.setFont( new Font( "Verdana", Font.PLAIN, 14 ) );
            websiteButton.setForeground( Color.blue );
            websiteButton.setBackground( lblIcon.getBackground() );
            websiteButton.setToolTipText( websiteUri.toString() );
            websiteButton.addActionListener( e -> openWebsite( websiteUri ) );
            websiteButton.setBounds( 78, 235, 400, 20 );
            websiteButton.setHorizontalAlignment( SwingConstants.LEFT );
            add( websiteButton );
        }


        @SneakyThrows
        public void openWebsite( URI websiteUri ) {
            Desktop.getDesktop().browse( websiteUri );
        }

    }

}
