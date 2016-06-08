/*! ******************************************************************************
*
* Pentaho Snowflake Plugin
*
* Author: Inquidia Consulting
*
* Copyright(c) 2016 Inquidia Consulting (www.inquidia.com)
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
******************************************************************************/

package org.inquidia.kettle.plugins.snowflakeplugin.warehousemanager;


import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.sql.ResultSet;


/**
 * Dialog that allows you to enter the settings for a Snowflake Warehouse Manager job entry.
 *
 * @author Chris
 * @since 2016-06-08
 */
@SuppressWarnings( "FieldCanBeLocal" )
public class WarehouseManagerDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = WarehouseManager.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private static final String[] MANAGEMENT_ACTION_DESCS = new String[]{
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Create" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Drop" )
  };

  private static final String[] WAREHOUSE_SIZE_DESCS = new String[]{
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Xsmall" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Small" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Medium" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Large" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Xlarge" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Xxlarge" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Size.Xxxlarge" )
  };

  private static final String[] WAREHOUSE_TYPE_DESCS = new String[]{
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Type.Standard" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Type.Enterprise" )
  };

  private WarehouseManager jobEntry;

  /**
   * Step name line
   */
  private Label wlName;
  private Text wName;
  private FormData fdlName, fdName;

  private CCombo wConnection;

  private Label wlWarehouseName;
  private ComboVar wWarehouseName;
  private FormData fdlWarehouseName, fdWarehouseName;

  private Label wlAction;
  private CCombo wAction;
  private FormData fdlAction, fdAction;

  private Group wCreateGroup;
  private FormData fdgCreateGroup;

  private Label wlCreateReplace;
  private Button wCreateReplace;
  private FormData fdlCreateReplace, fdCreateReplace;

  private Label wlCreateFailIfExists;
  private Button wCreateFailIfExists;
  private FormData fdlCreateFailIfExists, fdCreateFailIfExists;

  private Label wlCreateWarehouseSize;
  private ComboVar wCreateWarehouseSize;
  private FormData fdlCreateWarehouseSize, fdCreateWarehouseSize;

  private Label wlCreateWarehouseType;
  private ComboVar wCreateWarehouseType;
  private FormData fdlCreateWarehouseType, fdCreateWarehouseType;

  private Label wlCreateMaxClusterSize;
  private TextVar wCreateMaxClusterSize;
  private FormData fdlCreateMaxClusterSize, fdCreateMaxClusterSize;

  private Label wlCreateMinClusterSize;
  private TextVar wCreateMinClusterSize;
  private FormData fdlCreateMinClusterSize, fdCreateMinClusterSize;

  private Label wlCreateAutoSuspend;
  private TextVar wCreateAutoSuspend;
  private FormData fdlCreateAutoSuspend, fdCreateAutoSuspend;

  private Label wlCreateAutoResume;
  private Button wCreateAutoResume;
  private FormData fdlCreateAutoResume, fdCreateAutoResume;

  private Label wlCreateInitialSuspend;
  private Button wCreateInitialSuspend;
  private FormData fdlCreateInitialSuspend, fdCreateInitialSuspend;

  private Label wlCreateResourceMonitor;
  private ComboVar wCreateResourceMonitor;
  private FormData fdlCreateResourceMonitor, fdCreateResourceMonitor;

  private Label wlCreateComment;
  private TextVar wCreateComment;
  private FormData fdlCreateComment, fdCreateComment;

  private Group wDropGroup;
  private FormData fdgDropGroup;

  private Label wlDropFailIfNotExists;
  private Button wDropFailIfNotExists;
  private FormData fdlDropFailIfNotExists, fdDropFailIfNotExists;

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel;

  // private Shell shell;

  private SelectionAdapter lsDef;

  private boolean backupChanged;

  private Display display;

  public WarehouseManagerDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (WarehouseManager) jobEntryInt;
  }

  public JobEntryInterface open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };

    SelectionAdapter lsFlags = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        setFlags();
      }
    };

    lsDef = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        jobEntry.setChanged();
      }
    };

    backupChanged = jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Name.Label" ) );
    props.setLook( wlName );
    fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.top = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
    wlName.setLayoutData( fdlName );

    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    fdName = new FormData();
    fdName.top = new FormAttachment( 0, 0 );
    fdName.left = new FormAttachment( middle, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    // Connection line
    wConnection = addConnectionLine( shell, wName, middle, margin );
    if ( jobEntry.getDatabaseMeta() == null && jobMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    // Warehouse name line
    //
    wlWarehouseName = new Label( shell, SWT.RIGHT );
    wlWarehouseName.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.WarehouseName.Label" ) );
    props.setLook( wlWarehouseName );
    fdlWarehouseName = new FormData();
    fdlWarehouseName.left = new FormAttachment( 0, 0 );
    fdlWarehouseName.top = new FormAttachment( wConnection, margin * 2 );
    fdlWarehouseName.right = new FormAttachment( middle, -margin );
    wlWarehouseName.setLayoutData( fdlWarehouseName );

    wWarehouseName = new ComboVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWarehouseName );
    wWarehouseName.addModifyListener( lsMod );
    fdWarehouseName = new FormData();
    fdWarehouseName.left = new FormAttachment( middle, 0 );
    fdWarehouseName.top = new FormAttachment( wConnection, margin * 2 );
    fdWarehouseName.right = new FormAttachment( 100, 0 );
    wWarehouseName.setLayoutData( fdWarehouseName );
    wWarehouseName.addFocusListener( new FocusAdapter() {
      /**
       * Get the list of stages for the schema, and populate the stage name drop down.
       *
       * @param focusEvent The event
       */
      @Override
      public void focusGained( FocusEvent focusEvent ) {
        DatabaseMeta databaseMeta = jobMeta.findDatabase( wConnection.getText() );
        if ( databaseMeta != null ) {
          String warehouseName = wWarehouseName.getText();
          wWarehouseName.removeAll();
          Database db = null;
          try {
            db = new Database( jobMeta, databaseMeta );
            db.connect();
            ResultSet resultSet = db.openQuery( "show warehouses;", null, null, ResultSet.FETCH_FORWARD, false );
            RowMetaInterface rowMeta = db.getReturnRowMeta();
            Object[] row = db.getRow( resultSet );
            int nameField = rowMeta.indexOfValue( "NAME" );
            if ( nameField >= 0 ) {
              while ( row != null ) {
                String name = rowMeta.getString( row, nameField );
                wWarehouseName.add( name );
                row = db.getRow( resultSet );
              }
            } else {
              throw new KettleException( "Unable to find warehouse name field in result" );
            }
            db.closeQuery( resultSet );
            if ( warehouseName != null ) {
              wWarehouseName.setText( warehouseName );
            }
          } catch ( Exception ex ) {
            jobEntry.logDebug( "Error getting warehouses", ex );
          } finally {
            db.disconnect();
          }
        }

      }
    } );


    // ///////////////////
    // Action line
    // ///////////////////
    wlAction = new Label( shell, SWT.RIGHT );
    wlAction.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Label" ) );
    props.setLook( wlAction );
    fdlAction = new FormData();
    fdlAction.left = new FormAttachment( 0, 0 );
    fdlAction.right = new FormAttachment( middle, 0 );
    fdlAction.top = new FormAttachment( wWarehouseName, -margin );
    wlAction.setLayoutData( fdlAction );

    wAction = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAction.setItems( MANAGEMENT_ACTION_DESCS );
    props.setLook( wAction );
    fdAction = new FormData();
    fdAction.left = new FormAttachment( middle, 0 );
    fdAction.top = new FormAttachment( wWarehouseName, margin );
    fdAction.right = new FormAttachment( 100, 0 );
    wAction.setLayoutData( fdAction );
    wAction.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent modifyEvent ) {
        setFlags();
      }
    } );

    /////////////////////
    // Start Create Warehouse Group
    /////////////////////
    wCreateGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    wCreateGroup.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Group.CreateWarehouse.Label" ) );
    FormLayout createWarehouseLayout = new FormLayout();
    createWarehouseLayout.marginWidth = 3;
    createWarehouseLayout.marginHeight = 3;
    wCreateGroup.setLayout( createWarehouseLayout );
    props.setLook( wCreateGroup );

    fdgCreateGroup = new FormData();
    fdgCreateGroup.left = new FormAttachment( 0, 0 );
    fdgCreateGroup.right = new FormAttachment( 100, 0 );
    fdgCreateGroup.top = new FormAttachment( wAction, margin * 2 );
    fdgCreateGroup.bottom = new FormAttachment( 100, -margin * 2 );
    wCreateGroup.setLayoutData( fdgCreateGroup );

    // //////////////////////
    // Replace line
    // /////////////////////
    wlCreateReplace = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateReplace.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.Replace.Label" ) );
    wlCreateReplace.setToolTipText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.Replace.Tooltip" ) );
    props.setLook( wlCreateReplace );
    fdlCreateReplace = new FormData();
    fdlCreateReplace.left = new FormAttachment( 0, 0 );
    fdlCreateReplace.top = new FormAttachment( 0, margin );
    fdlCreateReplace.right = new FormAttachment( middle, 0 );
    wlCreateReplace.setLayoutData( fdlCreateReplace );

    wCreateReplace = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateReplace );
    fdCreateReplace = new FormData();
    fdCreateReplace.left = new FormAttachment( middle, margin );
    fdCreateReplace.top = new FormAttachment( 0, margin );
    fdCreateReplace.right = new FormAttachment( 100, 0 );
    wCreateReplace.setLayoutData( fdCreateReplace );
    wCreateReplace.addSelectionListener( lsDef );
    wCreateReplace.addSelectionListener( lsFlags );

    // /////////////////////
    // Fail if exists line
    // /////////////////////
    wlCreateFailIfExists = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateFailIfExists.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.FailIfExists.Label" ) );
    props.setLook( wlCreateFailIfExists );
    fdlCreateFailIfExists = new FormData();
    fdlCreateFailIfExists.left = new FormAttachment( 0, 0 );
    fdlCreateFailIfExists.top = new FormAttachment( wCreateReplace, margin );
    fdlCreateFailIfExists.right = new FormAttachment( middle, 0 );
    wlCreateFailIfExists.setLayoutData( fdlCreateFailIfExists );

    wCreateFailIfExists = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateFailIfExists );
    fdCreateFailIfExists = new FormData();
    fdCreateFailIfExists.left = new FormAttachment( middle, margin );
    fdCreateFailIfExists.top = new FormAttachment( wCreateReplace, margin );
    fdCreateFailIfExists.right = new FormAttachment( 100, 0 );
    wCreateFailIfExists.setLayoutData( fdCreateFailIfExists );
    wCreateFailIfExists.addSelectionListener( lsDef );

    // Warehouse Size
    //
    wlCreateWarehouseSize = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateWarehouseSize.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.CreateWarehouseSize.Label" ) );
    props.setLook( wlCreateWarehouseSize );
    fdlCreateWarehouseSize = new FormData();
    fdlCreateWarehouseSize.left = new FormAttachment( 0, 0 );
    fdlCreateWarehouseSize.top = new FormAttachment( wCreateFailIfExists, margin * 2 );
    fdlCreateWarehouseSize.right = new FormAttachment( middle, -margin );
    wlCreateWarehouseSize.setLayoutData( fdlCreateWarehouseSize );

    wCreateWarehouseSize = new ComboVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateWarehouseSize );
    wCreateWarehouseSize.addModifyListener( lsMod );
    fdCreateWarehouseSize = new FormData();
    fdCreateWarehouseSize.left = new FormAttachment( middle, 0 );
    fdCreateWarehouseSize.top = new FormAttachment( wCreateFailIfExists, margin * 2 );
    fdCreateWarehouseSize.right = new FormAttachment( 100, 0 );
    wCreateWarehouseSize.setLayoutData( fdCreateWarehouseSize );
    wCreateWarehouseSize.setItems( WAREHOUSE_SIZE_DESCS );

    // Warehouse Type
    //
    wlCreateWarehouseType = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateWarehouseType.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.CreateWarehouseType.Label" ) );
    props.setLook( wlCreateWarehouseType );
    fdlCreateWarehouseType = new FormData();
    fdlCreateWarehouseType.left = new FormAttachment( 0, 0 );
    fdlCreateWarehouseType.top = new FormAttachment( wCreateWarehouseSize, margin * 2 );
    fdlCreateWarehouseType.right = new FormAttachment( middle, -margin );
    wlCreateWarehouseType.setLayoutData( fdlCreateWarehouseType );

    wCreateWarehouseType = new ComboVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateWarehouseType );
    wCreateWarehouseType.addModifyListener( lsMod );
    fdCreateWarehouseType = new FormData();
    fdCreateWarehouseType.left = new FormAttachment( middle, 0 );
    fdCreateWarehouseType.top = new FormAttachment( wCreateWarehouseSize, margin * 2 );
    fdCreateWarehouseType.right = new FormAttachment( 100, 0 );
    wCreateWarehouseType.setLayoutData( fdCreateWarehouseType );
    wCreateWarehouseType.setEnabled( false );
    wCreateWarehouseType.setItems( WAREHOUSE_TYPE_DESCS );

    // /////////////////////
    // Max Cluster Size
    // /////////////////////
    wlCreateMaxClusterSize = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateMaxClusterSize.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.MaxClusterSize.Label" ) );
    props.setLook( wlCreateMaxClusterSize );
    fdlCreateMaxClusterSize = new FormData();
    fdlCreateMaxClusterSize.left = new FormAttachment( 0, 0 );
    fdlCreateMaxClusterSize.top = new FormAttachment( wCreateWarehouseType, margin );
    fdlCreateMaxClusterSize.right = new FormAttachment( middle, 0 );
    wlCreateMaxClusterSize.setLayoutData( fdlCreateMaxClusterSize );

    wCreateMaxClusterSize = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateMaxClusterSize.addModifyListener( lsMod );
    fdCreateMaxClusterSize = new FormData();
    fdCreateMaxClusterSize.left = new FormAttachment( middle, margin );
    fdCreateMaxClusterSize.right = new FormAttachment( 100, 0 );
    fdCreateMaxClusterSize.top = new FormAttachment( wCreateWarehouseType, margin );
    wCreateMaxClusterSize.setLayoutData( fdCreateMaxClusterSize );

    // /////////////////////
    // Min Cluster Size
    // /////////////////////
    wlCreateMinClusterSize = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateMinClusterSize.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.MinClusterSize.Label" ) );
    props.setLook( wlCreateMinClusterSize );
    fdlCreateMinClusterSize = new FormData();
    fdlCreateMinClusterSize.left = new FormAttachment( 0, 0 );
    fdlCreateMinClusterSize.top = new FormAttachment( wCreateMaxClusterSize, margin );
    fdlCreateMinClusterSize.right = new FormAttachment( middle, 0 );
    wlCreateMinClusterSize.setLayoutData( fdlCreateMinClusterSize );

    wCreateMinClusterSize = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateMinClusterSize.addModifyListener( lsMod );
    fdCreateMinClusterSize = new FormData();
    fdCreateMinClusterSize.left = new FormAttachment( middle, margin );
    fdCreateMinClusterSize.right = new FormAttachment( 100, 0 );
    fdCreateMinClusterSize.top = new FormAttachment( wCreateMaxClusterSize, margin );
    wCreateMinClusterSize.setLayoutData( fdCreateMinClusterSize );

    // /////////////////////
    // Auto Suspend Size
    // /////////////////////
    wlCreateAutoSuspend = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateAutoSuspend.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.AutoSuspend.Label" ) );
    wlCreateAutoSuspend.setToolTipText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.AutoSuspend.Tooltip" ) );
    props.setLook( wlCreateAutoSuspend );
    fdlCreateAutoSuspend = new FormData();
    fdlCreateAutoSuspend.left = new FormAttachment( 0, 0 );
    fdlCreateAutoSuspend.top = new FormAttachment( wCreateMinClusterSize, margin );
    fdlCreateAutoSuspend.right = new FormAttachment( middle, 0 );
    wlCreateAutoSuspend.setLayoutData( fdlCreateAutoSuspend );

    wCreateAutoSuspend = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateAutoSuspend.addModifyListener( lsMod );
    fdCreateAutoSuspend = new FormData();
    fdCreateAutoSuspend.left = new FormAttachment( middle, margin );
    fdCreateAutoSuspend.right = new FormAttachment( 100, 0 );
    fdCreateAutoSuspend.top = new FormAttachment( wCreateMinClusterSize, margin );
    wCreateAutoSuspend.setLayoutData( fdCreateAutoSuspend );

    // /////////////////////
    // Auto-resume
    // /////////////////////
    wlCreateAutoResume = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateAutoResume.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.AutoResume.Label" ) );
    props.setLook( wlCreateAutoResume );
    fdlCreateAutoResume = new FormData();
    fdlCreateAutoResume.left = new FormAttachment( 0, 0 );
    fdlCreateAutoResume.top = new FormAttachment( wCreateAutoSuspend, margin );
    fdlCreateAutoResume.right = new FormAttachment( middle, 0 );
    wlCreateAutoResume.setLayoutData( fdlCreateAutoResume );

    wCreateAutoResume = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateAutoResume );
    fdCreateAutoResume = new FormData();
    fdCreateAutoResume.left = new FormAttachment( middle, margin );
    fdCreateAutoResume.top = new FormAttachment( wCreateAutoSuspend, margin );
    fdCreateAutoResume.right = new FormAttachment( 100, 0 );
    wCreateAutoResume.setLayoutData( fdCreateAutoResume );
    wCreateAutoResume.addSelectionListener( lsDef );

    // /////////////////////
    // Auto-resume
    // /////////////////////
    wlCreateInitialSuspend = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateInitialSuspend.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.InitialSuspend.Label" ) );
    wlCreateInitialSuspend.setToolTipText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.InitialSuspend.Tooltip" ) );
    props.setLook( wlCreateInitialSuspend );
    fdlCreateInitialSuspend = new FormData();
    fdlCreateInitialSuspend.left = new FormAttachment( 0, 0 );
    fdlCreateInitialSuspend.top = new FormAttachment( wCreateAutoResume, margin );
    fdlCreateInitialSuspend.right = new FormAttachment( middle, 0 );
    wlCreateInitialSuspend.setLayoutData( fdlCreateInitialSuspend );

    wCreateInitialSuspend = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateInitialSuspend );
    fdCreateInitialSuspend = new FormData();
    fdCreateInitialSuspend.left = new FormAttachment( middle, margin );
    fdCreateInitialSuspend.top = new FormAttachment( wCreateAutoResume, margin );
    fdCreateInitialSuspend.right = new FormAttachment( 100, 0 );
    wCreateInitialSuspend.setLayoutData( fdCreateInitialSuspend );
    wCreateInitialSuspend.addSelectionListener( lsDef );

    // Resource monitor line
    //
    wlCreateResourceMonitor = new Label( shell, SWT.RIGHT );
    wlCreateResourceMonitor.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.ResourceMonitor.Label" ) );
    props.setLook( wlCreateResourceMonitor );
    fdlCreateResourceMonitor = new FormData();
    fdlCreateResourceMonitor.left = new FormAttachment( 0, 0 );
    fdlCreateResourceMonitor.top = new FormAttachment( wCreateInitialSuspend, margin * 2 );
    fdlCreateResourceMonitor.right = new FormAttachment( middle, -margin );
    wlCreateResourceMonitor.setLayoutData( fdlCreateResourceMonitor );

    wCreateResourceMonitor = new ComboVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateResourceMonitor );
    wCreateResourceMonitor.addModifyListener( lsMod );
    fdCreateResourceMonitor = new FormData();
    fdCreateResourceMonitor.left = new FormAttachment( middle, 0 );
    fdCreateResourceMonitor.top = new FormAttachment( wCreateInitialSuspend, margin * 2 );
    fdCreateResourceMonitor.right = new FormAttachment( 100, 0 );
    wCreateResourceMonitor.setLayoutData( fdCreateResourceMonitor );
    wCreateResourceMonitor.addFocusListener( new FocusAdapter() {
      /**
       * Get the list of stages for the schema, and populate the stage name drop down.
       *
       * @param focusEvent The event
       */
      @SuppressWarnings( "Duplicates" )
      @Override
      public void focusGained( FocusEvent focusEvent ) {
        DatabaseMeta databaseMeta = jobMeta.findDatabase( wConnection.getText() );
        if ( databaseMeta != null ) {
          String warehouseName = wWarehouseName.getText();
          wWarehouseName.removeAll();
          Database db = null;
          try {
            db = new Database( jobMeta, databaseMeta );
            db.connect();
            ResultSet resultSet = db.openQuery( "show resource monitors;", null, null, ResultSet.FETCH_FORWARD, false );
            RowMetaInterface rowMeta = db.getReturnRowMeta();
            Object[] row = db.getRow( resultSet );
            int nameField = rowMeta.indexOfValue( "NAME" );
            if ( nameField >= 0 ) {
              while ( row != null ) {
                String name = rowMeta.getString( row, nameField );
                wWarehouseName.add( name );
                row = db.getRow( resultSet );
              }
            } else {
              throw new KettleException( "Unable to find resource monitor name field in result" );
            }
            db.closeQuery( resultSet );
            if ( warehouseName != null ) {
              wWarehouseName.setText( warehouseName );
            }
          } catch ( Exception ex ) {
            jobEntry.logDebug( "Error getting resource monitors", ex );
          } finally {
            db.disconnect();
          }
        }

      }
    } );

    // /////////////////////
    //Comment Line
    // /////////////////////
    wlCreateComment = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateComment.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Create.Comment.Label" ) );
    props.setLook( wlCreateComment );
    fdlCreateComment = new FormData();
    fdlCreateComment.left = new FormAttachment( 0, 0 );
    fdlCreateComment.top = new FormAttachment( wCreateResourceMonitor, margin );
    fdlCreateComment.right = new FormAttachment( middle, 0 );
    wlCreateComment.setLayoutData( fdlCreateComment );

    wCreateComment = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateComment.addModifyListener( lsMod );
    fdCreateComment = new FormData();
    fdCreateComment.left = new FormAttachment( middle, margin );
    fdCreateComment.right = new FormAttachment( 100, 0 );
    fdCreateComment.top = new FormAttachment( wCreateResourceMonitor, margin );
    wCreateComment.setLayoutData( fdCreateComment );

    /////////////////////
    // Start Drop Warehouse Group
    /////////////////////
    wDropGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    wDropGroup.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Group.DropWarehouse.Label" ) );
    FormLayout dropWarehouseLayout = new FormLayout();
    dropWarehouseLayout.marginWidth = 3;
    dropWarehouseLayout.marginHeight = 3;
    wCreateGroup.setLayout( dropWarehouseLayout );
    props.setLook( wDropGroup );

    fdgDropGroup = new FormData();
    fdgDropGroup.left = new FormAttachment( 0, 0 );
    fdgDropGroup.right = new FormAttachment( 100, 0 );
    fdgDropGroup.top = new FormAttachment( wAction, margin * 2 );
    fdgDropGroup.bottom = new FormAttachment( 100, -margin * 2 );
    wDropGroup.setLayoutData( fdgDropGroup );

    // /////////////////////
    // Fail if exists line
    // /////////////////////
    wlDropFailIfNotExists = new Label( wDropGroup, SWT.RIGHT );
    wlDropFailIfNotExists.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Drop.FailIfExists.Label" ) );
    props.setLook( wlDropFailIfNotExists );
    fdlDropFailIfNotExists = new FormData();
    fdlDropFailIfNotExists.left = new FormAttachment( 0, 0 );
    fdlDropFailIfNotExists.top = new FormAttachment( 0, margin );
    fdlDropFailIfNotExists.right = new FormAttachment( middle, 0 );
    wlDropFailIfNotExists.setLayoutData( fdlDropFailIfNotExists );

    wDropFailIfNotExists = new Button( wDropGroup, SWT.CHECK );
    props.setLook( wDropFailIfNotExists );
    fdDropFailIfNotExists = new FormData();
    fdDropFailIfNotExists.left = new FormAttachment( middle, margin );
    fdDropFailIfNotExists.top = new FormAttachment( 0, margin );
    fdDropFailIfNotExists.right = new FormAttachment( 100, 0 );
    wDropFailIfNotExists.setLayoutData( fdDropFailIfNotExists );
    wDropFailIfNotExists.addSelectionListener( lsDef );


    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[]{ wOK, wCancel }, margin, wCreateGroup );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );




    wName.addSelectionListener( lsDef );
    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setFlags();

    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "WarehouseManagerSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return jobEntry;
  }

  public void setFlags() {
    wCreateFailIfExists.setEnabled( !wCreateReplace.getSelection() );
    wCreateGroup.setVisible( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_CREATE );
    wDropGroup.setVisible( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_DROP );

  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( jobEntry.getName(), "" ) );
    wConnection.setText( jobEntry.getDatabaseMeta() != null ? jobEntry.getDatabaseMeta().getName() : "" );
    wWarehouseName.setText( Const.NVL( jobEntry.getWarehouseName(), "" ) );
    int actionId = jobEntry.getManagementActionId();
    if ( actionId >= 0 && actionId < MANAGEMENT_ACTION_DESCS.length ) {
      wAction.setText( MANAGEMENT_ACTION_DESCS[actionId] );
    }
    wCreateReplace.setSelection( jobEntry.isReplace() );
    wCreateFailIfExists.setSelection( jobEntry.isFailIfExists() );
    int warehouseSizeId = jobEntry.getWarehouseSizeId();
    if ( warehouseSizeId >= 0 && warehouseSizeId < WAREHOUSE_SIZE_DESCS.length ) {
      wCreateWarehouseSize.setText( WAREHOUSE_SIZE_DESCS[warehouseSizeId] );
    } else {
      wCreateWarehouseSize.setText( Const.NVL( jobEntry.getWarehouseSize(), "" ) );
    }
    int warehouseTypeId = jobEntry.getWarehouseTypeId();
    if ( warehouseTypeId >= 0 && warehouseTypeId < WAREHOUSE_TYPE_DESCS.length ) {
      wCreateWarehouseType.setText( WAREHOUSE_TYPE_DESCS[warehouseTypeId] );
    } else {
      wCreateWarehouseType.setText( Const.NVL( jobEntry.getWarehouseType(), "" ) );
    }
    wCreateMaxClusterSize.setText( Const.NVL( jobEntry.getMaxClusterCount(), "" ) );
    wCreateMinClusterSize.setText( Const.NVL( jobEntry.getMinClusterCount(), "" ) );
    wCreateAutoSuspend.setText( Const.NVL( jobEntry.getAutoSuspend(), "" ) );
    wCreateAutoResume.setSelection( jobEntry.isAutoResume() );
    wCreateInitialSuspend.setSelection( jobEntry.isInitiallySuspended() );
    wCreateResourceMonitor.setText( Const.NVL( jobEntry.getResourceMonitor(), "" ) );
    wCreateComment.setText( Const.NVL( jobEntry.getComment(), "" ) );

    wDropFailIfNotExists.setSelection( jobEntry.isFailIfNotExists() );

    if ( Const.isEmpty( wAction.getText() ) ) {
      wAction.setText( MANAGEMENT_ACTION_DESCS[0] );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( backupChanged );

    jobEntry = null;
    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( wName.getText() );
    jobEntry.setDatabaseMeta( jobMeta.findDatabase( Const.NVL( wConnection.getText(), "" ) ) );
    jobEntry.setWarehouseName( wWarehouseName.getText() );
    jobEntry.setManagementActionById( wAction.getSelectionIndex() );
    if ( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_CREATE ) {
      jobEntry.setReplace( wCreateReplace.getSelection() );
      jobEntry.setFailIfExists( wCreateFailIfExists.getSelection() );
      boolean warehouseSizeFound = false;
      for ( int i = 0; i < WAREHOUSE_SIZE_DESCS.length; i++ ) {
        if ( wCreateWarehouseSize.getText().equals( WAREHOUSE_SIZE_DESCS[i] ) ) {
          warehouseSizeFound = true;
          jobEntry.setWarehouseSizeById( i );
          break;
        }
      }
      if ( !warehouseSizeFound ) {
        jobEntry.setWarehouseSize( wCreateWarehouseSize.getText() );
      }

      boolean warehouseTypeFound = false;
      for ( int i = 0; i < WAREHOUSE_TYPE_DESCS.length; i++ ) {
        if ( wCreateWarehouseType.getText().equals( WAREHOUSE_TYPE_DESCS[i] ) ) {
          warehouseTypeFound = true;
          jobEntry.setWarehouseTypeById( i );
          break;
        }
      }
      if ( !warehouseTypeFound ) {
        jobEntry.setWarehouseType( wCreateWarehouseType.getText() );
      }

      jobEntry.setMaxClusterCount( wCreateMaxClusterSize.getText() );
      jobEntry.setMinClusterCount( wCreateMinClusterSize.getText() );
      jobEntry.setAutoResume( wCreateAutoResume.getSelection() );
      jobEntry.setAutoSuspend( wCreateAutoSuspend.getText() );
      jobEntry.setInitiallySuspended( wCreateInitialSuspend.getSelection() );
      jobEntry.setResourceMonitor( wCreateResourceMonitor.getText() );
      jobEntry.setComment( wCreateComment.getText() );
    } else if ( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_DROP ) {
      jobEntry.setFailIfNotExists( wDropFailIfNotExists.getSelection() );
    }

    dispose();
  }
}
