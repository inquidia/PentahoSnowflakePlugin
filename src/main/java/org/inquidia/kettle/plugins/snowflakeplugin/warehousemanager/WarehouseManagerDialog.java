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

package main.java.org.inquidia.kettle.plugins.snowflakeplugin.warehousemanager;


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
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
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
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Drop" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Resume" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Suspend" ),
    BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Action.Alter" )
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

  private Group wResumeGroup;
  private FormData fdgResumeGroup;

  private Label wlResumeFailIfNotExists;
  private Button wResumeFailIfNotExists;
  private FormData fdlResumeFailIfNotExists, fdResumeFailIfNotExists;

  private Group wSuspendGroup;
  private FormData fdgSuspendGroup;

  private Label wlSuspendFailIfNotExists;
  private Button wSuspendFailIfNotExists;
  private FormData fdlSuspendFailIfNotExists, fdSuspendFailIfNotExists;

  private Group wAlterGroup;
  private FormData fdgAlterGroup;

  private Label wlAlterFailIfNotExists;
  private Button wAlterFailIfNotExists;
  private FormData fdlAlterFailIfNotExists, fdAlterFailIfNotExists;

  private Label wlAlterWarehouseSize;
  private ComboVar wAlterWarehouseSize;
  private FormData fdlAlterWarehouseSize, fdAlterWarehouseSize;

  private Label wlAlterWarehouseType;
  private ComboVar wAlterWarehouseType;
  private FormData fdlAlterWarehouseType, fdAlterWarehouseType;

  private Label wlAlterMaxClusterSize;
  private TextVar wAlterMaxClusterSize;
  private FormData fdlAlterMaxClusterSize, fdAlterMaxClusterSize;

  private Label wlAlterMinClusterSize;
  private TextVar wAlterMinClusterSize;
  private FormData fdlAlterMinClusterSize, fdAlterMinClusterSize;

  private Label wlAlterAutoSuspend;
  private TextVar wAlterAutoSuspend;
  private FormData fdlAlterAutoSuspend, fdAlterAutoSuspend;

  private Label wlAlterAutoResume;
  private Button wAlterAutoResume;
  private FormData fdlAlterAutoResume, fdAlterAutoResume;

  private Label wlAlterResourceMonitor;
  private ComboVar wAlterResourceMonitor;
  private FormData fdlAlterResourceMonitor, fdAlterResourceMonitor;

  private Label wlAlterComment;
  private TextVar wAlterComment;
  private FormData fdlAlterComment, fdAlterComment;


  private Link wDevelopedBy;
  private FormData fdDevelopedBy;

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
    fdlAction.right = new FormAttachment( middle, -margin );
    fdlAction.top = new FormAttachment( wWarehouseName, margin );
    wlAction.setLayoutData( fdlAction );

    wAction = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wAction.setItems( MANAGEMENT_ACTION_DESCS );
    props.setLook( wAction );
    fdAction = new FormData();
    fdAction.left = new FormAttachment( middle, 0 );
    fdAction.top = new FormAttachment( wWarehouseName, margin );
    fdAction.right = new FormAttachment( 100, 0 );
    wAction.setLayoutData( fdAction );
    wAction.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        setFlags();
      }
    } );
    wAction.addModifyListener( lsMod );

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
    // fdgCreateGroup.bottom = new FormAttachment( 100, -margin * 2 );
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
    fdlCreateReplace.right = new FormAttachment( middle, -margin );
    wlCreateReplace.setLayoutData( fdlCreateReplace );

    wCreateReplace = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateReplace );
    fdCreateReplace = new FormData();
    fdCreateReplace.left = new FormAttachment( middle, 0 );
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
    fdlCreateFailIfExists.right = new FormAttachment( middle, -margin );
    wlCreateFailIfExists.setLayoutData( fdlCreateFailIfExists );

    wCreateFailIfExists = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateFailIfExists );
    fdCreateFailIfExists = new FormData();
    fdCreateFailIfExists.left = new FormAttachment( middle, 0 );
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
    fdlCreateWarehouseSize.top = new FormAttachment( wCreateFailIfExists, margin );
    fdlCreateWarehouseSize.right = new FormAttachment( middle, -margin );
    wlCreateWarehouseSize.setLayoutData( fdlCreateWarehouseSize );

    wCreateWarehouseSize = new ComboVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateWarehouseSize );
    wCreateWarehouseSize.addModifyListener( lsMod );
    fdCreateWarehouseSize = new FormData();
    fdCreateWarehouseSize.left = new FormAttachment( middle, 0 );
    fdCreateWarehouseSize.top = new FormAttachment( wCreateFailIfExists, margin );
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
    fdlCreateWarehouseType.top = new FormAttachment( wCreateWarehouseSize, margin );
    fdlCreateWarehouseType.right = new FormAttachment( middle, -margin );
    wlCreateWarehouseType.setLayoutData( fdlCreateWarehouseType );

    wCreateWarehouseType = new ComboVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateWarehouseType );
    wCreateWarehouseType.addModifyListener( lsMod );
    fdCreateWarehouseType = new FormData();
    fdCreateWarehouseType.left = new FormAttachment( middle, 0 );
    fdCreateWarehouseType.top = new FormAttachment( wCreateWarehouseSize, margin );
    fdCreateWarehouseType.right = new FormAttachment( 100, 0 );
    wCreateWarehouseType.setLayoutData( fdCreateWarehouseType );
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
    fdlCreateMaxClusterSize.right = new FormAttachment( middle, -margin );
    wlCreateMaxClusterSize.setLayoutData( fdlCreateMaxClusterSize );

    wCreateMaxClusterSize = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateMaxClusterSize.addModifyListener( lsMod );
    fdCreateMaxClusterSize = new FormData();
    fdCreateMaxClusterSize.left = new FormAttachment( middle, 0 );
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
    fdlCreateMinClusterSize.right = new FormAttachment( middle, -margin );
    wlCreateMinClusterSize.setLayoutData( fdlCreateMinClusterSize );

    wCreateMinClusterSize = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateMinClusterSize.addModifyListener( lsMod );
    fdCreateMinClusterSize = new FormData();
    fdCreateMinClusterSize.left = new FormAttachment( middle, 0 );
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
    fdlCreateAutoSuspend.right = new FormAttachment( middle, -margin );
    wlCreateAutoSuspend.setLayoutData( fdlCreateAutoSuspend );

    wCreateAutoSuspend = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateAutoSuspend.addModifyListener( lsMod );
    fdCreateAutoSuspend = new FormData();
    fdCreateAutoSuspend.left = new FormAttachment( middle, 0 );
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
    fdlCreateAutoResume.right = new FormAttachment( middle, -margin );
    wlCreateAutoResume.setLayoutData( fdlCreateAutoResume );

    wCreateAutoResume = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateAutoResume );
    fdCreateAutoResume = new FormData();
    fdCreateAutoResume.left = new FormAttachment( middle, 0 );
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
    fdlCreateInitialSuspend.right = new FormAttachment( middle, -margin );
    wlCreateInitialSuspend.setLayoutData( fdlCreateInitialSuspend );

    wCreateInitialSuspend = new Button( wCreateGroup, SWT.CHECK );
    props.setLook( wCreateInitialSuspend );
    fdCreateInitialSuspend = new FormData();
    fdCreateInitialSuspend.left = new FormAttachment( middle, 0 );
    fdCreateInitialSuspend.top = new FormAttachment( wCreateAutoResume, margin );
    fdCreateInitialSuspend.right = new FormAttachment( 100, 0 );
    wCreateInitialSuspend.setLayoutData( fdCreateInitialSuspend );
    wCreateInitialSuspend.addSelectionListener( lsDef );

    // Resource monitor line
    //
    wlCreateResourceMonitor = new Label( wCreateGroup, SWT.RIGHT );
    wlCreateResourceMonitor.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.ResourceMonitor.Label" ) );
    props.setLook( wlCreateResourceMonitor );
    fdlCreateResourceMonitor = new FormData();
    fdlCreateResourceMonitor.left = new FormAttachment( 0, 0 );
    fdlCreateResourceMonitor.top = new FormAttachment( wCreateInitialSuspend, margin );
    fdlCreateResourceMonitor.right = new FormAttachment( middle, -margin );
    wlCreateResourceMonitor.setLayoutData( fdlCreateResourceMonitor );

    wCreateResourceMonitor = new ComboVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateResourceMonitor );
    wCreateResourceMonitor.addModifyListener( lsMod );
    fdCreateResourceMonitor = new FormData();
    fdCreateResourceMonitor.left = new FormAttachment( middle, 0 );
    fdCreateResourceMonitor.top = new FormAttachment( wCreateInitialSuspend, margin );
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
        getResourceMonitors();
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
    fdlCreateComment.right = new FormAttachment( middle, -margin );
    wlCreateComment.setLayoutData( fdlCreateComment );

    wCreateComment = new TextVar( jobMeta, wCreateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateGroup );
    wCreateComment.addModifyListener( lsMod );
    fdCreateComment = new FormData();
    fdCreateComment.left = new FormAttachment( middle, 0 );
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
    wDropGroup.setLayout( dropWarehouseLayout );
    props.setLook( wDropGroup );

    fdgDropGroup = new FormData();
    fdgDropGroup.left = new FormAttachment( 0, 0 );
    fdgDropGroup.right = new FormAttachment( 100, 0 );
    fdgDropGroup.top = new FormAttachment( wAction, margin * 2 );
    // fdgCreateGroup.bottom = new FormAttachment( 100, -margin * 2 );
    wDropGroup.setLayoutData( fdgDropGroup );

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    wlDropFailIfNotExists = new Label( wDropGroup, SWT.RIGHT );
    wlDropFailIfNotExists.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Drop.FailIfNotExists.Label" ) );
    props.setLook( wlDropFailIfNotExists );
    fdlDropFailIfNotExists = new FormData();
    fdlDropFailIfNotExists.left = new FormAttachment( 0, 0 );
    fdlDropFailIfNotExists.top = new FormAttachment( 0, margin );
    fdlDropFailIfNotExists.right = new FormAttachment( middle, -margin );
    wlDropFailIfNotExists.setLayoutData( fdlDropFailIfNotExists );

    wDropFailIfNotExists = new Button( wDropGroup, SWT.CHECK );
    props.setLook( wDropFailIfNotExists );
    fdDropFailIfNotExists = new FormData();
    fdDropFailIfNotExists.left = new FormAttachment( middle, 0 );
    fdDropFailIfNotExists.top = new FormAttachment( 0, margin );
    fdDropFailIfNotExists.right = new FormAttachment( 100, 0 );
    wDropFailIfNotExists.setLayoutData( fdDropFailIfNotExists );
    wDropFailIfNotExists.addSelectionListener( lsDef );

    /////////////////////
    // Start Resume Warehouse Group
    /////////////////////
    wResumeGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    wResumeGroup.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Group.ResumeWarehouse.Label" ) );
    FormLayout resumeWarehouseLayout = new FormLayout();
    resumeWarehouseLayout.marginWidth = 3;
    resumeWarehouseLayout.marginHeight = 3;
    wResumeGroup.setLayout( resumeWarehouseLayout );
    props.setLook( wResumeGroup );

    fdgResumeGroup = new FormData();
    fdgResumeGroup.left = new FormAttachment( 0, 0 );
    fdgResumeGroup.right = new FormAttachment( 100, 0 );
    fdgResumeGroup.top = new FormAttachment( wAction, margin * 2 );
    // fdgCreateGroup.bottom = new FormAttachment( 100, -margin * 2 );
    wResumeGroup.setLayoutData( fdgResumeGroup );

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    wlResumeFailIfNotExists = new Label( wResumeGroup, SWT.RIGHT );
    wlResumeFailIfNotExists.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Resume.FailIfNotExists.Label" ) );
    props.setLook( wlResumeFailIfNotExists );
    fdlResumeFailIfNotExists = new FormData();
    fdlResumeFailIfNotExists.left = new FormAttachment( 0, 0 );
    fdlResumeFailIfNotExists.top = new FormAttachment( 0, margin );
    fdlResumeFailIfNotExists.right = new FormAttachment( middle, -margin );
    wlResumeFailIfNotExists.setLayoutData( fdlResumeFailIfNotExists );

    wResumeFailIfNotExists = new Button( wResumeGroup, SWT.CHECK );
    props.setLook( wResumeFailIfNotExists );
    fdResumeFailIfNotExists = new FormData();
    fdResumeFailIfNotExists.left = new FormAttachment( middle, 0 );
    fdResumeFailIfNotExists.top = new FormAttachment( 0, margin );
    fdResumeFailIfNotExists.right = new FormAttachment( 100, 0 );
    wResumeFailIfNotExists.setLayoutData( fdResumeFailIfNotExists );
    wResumeFailIfNotExists.addSelectionListener( lsDef );


    /////////////////////
    // Start Suspend Warehouse Group
    /////////////////////
    wSuspendGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    wSuspendGroup.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Group.SuspendWarehouse.Label" ) );
    FormLayout suspendWarehouseLayout = new FormLayout();
    suspendWarehouseLayout.marginWidth = 3;
    suspendWarehouseLayout.marginHeight = 3;
    wSuspendGroup.setLayout( suspendWarehouseLayout );
    props.setLook( wSuspendGroup );

    fdgSuspendGroup = new FormData();
    fdgSuspendGroup.left = new FormAttachment( 0, 0 );
    fdgSuspendGroup.right = new FormAttachment( 100, 0 );
    fdgSuspendGroup.top = new FormAttachment( wAction, margin * 2 );
    // fdgCreateGroup.bottom = new FormAttachment( 100, -margin * 2 );
    wSuspendGroup.setLayoutData( fdgSuspendGroup );

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    wlSuspendFailIfNotExists = new Label( wSuspendGroup, SWT.RIGHT );
    wlSuspendFailIfNotExists.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Suspend.FailIfNotExists.Label" ) );
    props.setLook( wlSuspendFailIfNotExists );
    fdlSuspendFailIfNotExists = new FormData();
    fdlSuspendFailIfNotExists.left = new FormAttachment( 0, 0 );
    fdlSuspendFailIfNotExists.top = new FormAttachment( 0, margin );
    fdlSuspendFailIfNotExists.right = new FormAttachment( middle, -margin );
    wlSuspendFailIfNotExists.setLayoutData( fdlSuspendFailIfNotExists );

    wSuspendFailIfNotExists = new Button( wSuspendGroup, SWT.CHECK );
    props.setLook( wSuspendFailIfNotExists );
    fdSuspendFailIfNotExists = new FormData();
    fdSuspendFailIfNotExists.left = new FormAttachment( middle, 0 );
    fdSuspendFailIfNotExists.top = new FormAttachment( 0, margin );
    fdSuspendFailIfNotExists.right = new FormAttachment( 100, 0 );
    wSuspendFailIfNotExists.setLayoutData( fdSuspendFailIfNotExists );
    wSuspendFailIfNotExists.addSelectionListener( lsDef );

    /////////////////////
    // Start Alter Warehouse Group
    /////////////////////
    wAlterGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    wAlterGroup.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Group.AlterWarehouse.Label" ) );
    FormLayout alterWarehouseLayout = new FormLayout();
    alterWarehouseLayout.marginWidth = 3;
    alterWarehouseLayout.marginHeight = 3;
    wAlterGroup.setLayout( alterWarehouseLayout );
    props.setLook( wAlterGroup );

    fdgAlterGroup = new FormData();
    fdgAlterGroup.left = new FormAttachment( 0, 0 );
    fdgAlterGroup.right = new FormAttachment( 100, 0 );
    fdgAlterGroup.top = new FormAttachment( wAction, margin * 2 );
    // fdgAlterGroup.bottom = new FormAttachment( 100, -margin * 2 );
    wAlterGroup.setLayoutData( fdgAlterGroup );

    // //////////////////////
    // Fail if Not exists line
    // /////////////////////
    wlAlterFailIfNotExists = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterFailIfNotExists.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.FailIfNotExists.Label" ) );
    props.setLook( wlAlterFailIfNotExists );
    fdlAlterFailIfNotExists = new FormData();
    fdlAlterFailIfNotExists.left = new FormAttachment( 0, 0 );
    fdlAlterFailIfNotExists.top = new FormAttachment( 0, margin );
    fdlAlterFailIfNotExists.right = new FormAttachment( middle, -margin );
    wlAlterFailIfNotExists.setLayoutData( fdlAlterFailIfNotExists );

    wAlterFailIfNotExists = new Button( wAlterGroup, SWT.CHECK );
    props.setLook( wAlterFailIfNotExists );
    fdAlterFailIfNotExists = new FormData();
    fdAlterFailIfNotExists.left = new FormAttachment( middle, 0 );
    fdAlterFailIfNotExists.top = new FormAttachment( 0, margin );
    fdAlterFailIfNotExists.right = new FormAttachment( 100, 0 );
    wAlterFailIfNotExists.setLayoutData( fdAlterFailIfNotExists );
    wAlterFailIfNotExists.addSelectionListener( lsDef );

    // Warehouse Size
    //
    wlAlterWarehouseSize = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterWarehouseSize.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.AlterWarehouseSize.Label" ) );
    props.setLook( wlAlterWarehouseSize );
    fdlAlterWarehouseSize = new FormData();
    fdlAlterWarehouseSize.left = new FormAttachment( 0, 0 );
    fdlAlterWarehouseSize.top = new FormAttachment( wAlterFailIfNotExists, margin );
    fdlAlterWarehouseSize.right = new FormAttachment( middle, -margin );
    wlAlterWarehouseSize.setLayoutData( fdlAlterWarehouseSize );

    wAlterWarehouseSize = new ComboVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterWarehouseSize );
    wAlterWarehouseSize.addModifyListener( lsMod );
    fdAlterWarehouseSize = new FormData();
    fdAlterWarehouseSize.left = new FormAttachment( middle, 0 );
    fdAlterWarehouseSize.top = new FormAttachment( wAlterFailIfNotExists, margin );
    fdAlterWarehouseSize.right = new FormAttachment( 100, 0 );
    wAlterWarehouseSize.setLayoutData( fdAlterWarehouseSize );
    wAlterWarehouseSize.setItems( WAREHOUSE_SIZE_DESCS );

    // Warehouse Type
    //
    wlAlterWarehouseType = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterWarehouseType.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.AlterWarehouseType.Label" ) );
    props.setLook( wlAlterWarehouseType );
    fdlAlterWarehouseType = new FormData();
    fdlAlterWarehouseType.left = new FormAttachment( 0, 0 );
    fdlAlterWarehouseType.top = new FormAttachment( wAlterWarehouseSize, margin );
    fdlAlterWarehouseType.right = new FormAttachment( middle, -margin );
    wlAlterWarehouseType.setLayoutData( fdlAlterWarehouseType );

    wAlterWarehouseType = new ComboVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterWarehouseType );
    wAlterWarehouseType.addModifyListener( lsMod );
    fdAlterWarehouseType = new FormData();
    fdAlterWarehouseType.left = new FormAttachment( middle, 0 );
    fdAlterWarehouseType.top = new FormAttachment( wAlterWarehouseSize, margin );
    fdAlterWarehouseType.right = new FormAttachment( 100, 0 );
    wAlterWarehouseType.setLayoutData( fdAlterWarehouseType );
    wAlterWarehouseType.setItems( WAREHOUSE_TYPE_DESCS );

    // /////////////////////
    // Max Cluster Size
    // /////////////////////
    wlAlterMaxClusterSize = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterMaxClusterSize.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.MaxClusterSize.Label" ) );
    props.setLook( wlAlterMaxClusterSize );
    fdlAlterMaxClusterSize = new FormData();
    fdlAlterMaxClusterSize.left = new FormAttachment( 0, 0 );
    fdlAlterMaxClusterSize.top = new FormAttachment( wAlterWarehouseType, margin );
    fdlAlterMaxClusterSize.right = new FormAttachment( middle, -margin );
    wlAlterMaxClusterSize.setLayoutData( fdlAlterMaxClusterSize );

    wAlterMaxClusterSize = new TextVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterGroup );
    wAlterMaxClusterSize.addModifyListener( lsMod );
    fdAlterMaxClusterSize = new FormData();
    fdAlterMaxClusterSize.left = new FormAttachment( middle, 0 );
    fdAlterMaxClusterSize.right = new FormAttachment( 100, 0 );
    fdAlterMaxClusterSize.top = new FormAttachment( wAlterWarehouseType, margin );
    wAlterMaxClusterSize.setLayoutData( fdAlterMaxClusterSize );

    // /////////////////////
    // Min Cluster Size
    // /////////////////////
    wlAlterMinClusterSize = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterMinClusterSize.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.MinClusterSize.Label" ) );
    props.setLook( wlAlterMinClusterSize );
    fdlAlterMinClusterSize = new FormData();
    fdlAlterMinClusterSize.left = new FormAttachment( 0, 0 );
    fdlAlterMinClusterSize.top = new FormAttachment( wAlterMaxClusterSize, margin );
    fdlAlterMinClusterSize.right = new FormAttachment( middle, -margin );
    wlAlterMinClusterSize.setLayoutData( fdlAlterMinClusterSize );

    wAlterMinClusterSize = new TextVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterGroup );
    wAlterMinClusterSize.addModifyListener( lsMod );
    fdAlterMinClusterSize = new FormData();
    fdAlterMinClusterSize.left = new FormAttachment( middle, 0 );
    fdAlterMinClusterSize.right = new FormAttachment( 100, 0 );
    fdAlterMinClusterSize.top = new FormAttachment( wAlterMaxClusterSize, margin );
    wAlterMinClusterSize.setLayoutData( fdAlterMinClusterSize );

    // /////////////////////
    // Auto Suspend Size
    // /////////////////////
    wlAlterAutoSuspend = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterAutoSuspend.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.AutoSuspend.Label" ) );
    wlAlterAutoSuspend.setToolTipText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.AutoSuspend.Tooltip" ) );
    props.setLook( wlAlterAutoSuspend );
    fdlAlterAutoSuspend = new FormData();
    fdlAlterAutoSuspend.left = new FormAttachment( 0, 0 );
    fdlAlterAutoSuspend.top = new FormAttachment( wAlterMinClusterSize, margin );
    fdlAlterAutoSuspend.right = new FormAttachment( middle, -margin );
    wlAlterAutoSuspend.setLayoutData( fdlAlterAutoSuspend );

    wAlterAutoSuspend = new TextVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterGroup );
    wAlterAutoSuspend.addModifyListener( lsMod );
    fdAlterAutoSuspend = new FormData();
    fdAlterAutoSuspend.left = new FormAttachment( middle, 0 );
    fdAlterAutoSuspend.right = new FormAttachment( 100, 0 );
    fdAlterAutoSuspend.top = new FormAttachment( wAlterMinClusterSize, margin );
    wAlterAutoSuspend.setLayoutData( fdAlterAutoSuspend );

    // /////////////////////
    // Auto-resume
    // /////////////////////
    wlAlterAutoResume = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterAutoResume.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.AutoResume.Label" ) );
    props.setLook( wlAlterAutoResume );
    fdlAlterAutoResume = new FormData();
    fdlAlterAutoResume.left = new FormAttachment( 0, 0 );
    fdlAlterAutoResume.top = new FormAttachment( wAlterAutoSuspend, margin );
    fdlAlterAutoResume.right = new FormAttachment( middle, -margin );
    wlAlterAutoResume.setLayoutData( fdlAlterAutoResume );

    wAlterAutoResume = new Button( wAlterGroup, SWT.CHECK );
    props.setLook( wAlterAutoResume );
    fdAlterAutoResume = new FormData();
    fdAlterAutoResume.left = new FormAttachment( middle, 0 );
    fdAlterAutoResume.top = new FormAttachment( wAlterAutoSuspend, margin );
    fdAlterAutoResume.right = new FormAttachment( 100, 0 );
    wAlterAutoResume.setLayoutData( fdAlterAutoResume );
    wAlterAutoResume.addSelectionListener( lsDef );

    // Resource monitor line
    //
    wlAlterResourceMonitor = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterResourceMonitor.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.ResourceMonitor.Label" ) );
    props.setLook( wlAlterResourceMonitor );
    fdlAlterResourceMonitor = new FormData();
    fdlAlterResourceMonitor.left = new FormAttachment( 0, 0 );
    fdlAlterResourceMonitor.top = new FormAttachment( wAlterAutoResume, margin );
    fdlAlterResourceMonitor.right = new FormAttachment( middle, -margin );
    wlAlterResourceMonitor.setLayoutData( fdlAlterResourceMonitor );

    wAlterResourceMonitor = new ComboVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterResourceMonitor );
    wAlterResourceMonitor.addModifyListener( lsMod );
    fdAlterResourceMonitor = new FormData();
    fdAlterResourceMonitor.left = new FormAttachment( middle, 0 );
    fdAlterResourceMonitor.top = new FormAttachment( wAlterAutoResume, margin );
    fdAlterResourceMonitor.right = new FormAttachment( 100, 0 );
    wAlterResourceMonitor.setLayoutData( fdAlterResourceMonitor );
    wAlterResourceMonitor.addFocusListener( new FocusAdapter() {
      /**
       * Get the list of stages for the schema, and populate the stage name drop down.
       *
       * @param focusEvent The event
       */
      @SuppressWarnings( "Duplicates" )
      @Override
      public void focusGained( FocusEvent focusEvent ) {
        getResourceMonitors();

      }
    } );

    // /////////////////////
    //Comment Line
    // /////////////////////
    wlAlterComment = new Label( wAlterGroup, SWT.RIGHT );
    wlAlterComment.setText( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Dialog.Alter.Comment.Label" ) );
    props.setLook( wlAlterComment );
    fdlAlterComment = new FormData();
    fdlAlterComment.left = new FormAttachment( 0, 0 );
    fdlAlterComment.top = new FormAttachment( wAlterResourceMonitor, margin );
    fdlAlterComment.right = new FormAttachment( middle, -margin );
    wlAlterComment.setLayoutData( fdlAlterComment );

    wAlterComment = new TextVar( jobMeta, wAlterGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAlterGroup );
    wAlterComment.addModifyListener( lsMod );
    fdAlterComment = new FormData();
    fdAlterComment.left = new FormAttachment( middle, 0 );
    fdAlterComment.right = new FormAttachment( 100, 0 );
    fdAlterComment.top = new FormAttachment( wAlterResourceMonitor, margin );
    wAlterComment.setLayoutData( fdAlterComment );


    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[]{ wOK, wCancel }, margin, wCreateGroup );

    wDevelopedBy = new Link( shell, SWT.PUSH );
    wDevelopedBy.setText( "Developed by Inquidia Consulting (<a href=\"http://www.inquidia.com\">www.inquidia.com</a>)" );
    fdDevelopedBy = new FormData();
    fdDevelopedBy.right = new FormAttachment( 100, margin );
    fdDevelopedBy.bottom = new FormAttachment( 100, margin );
    wDevelopedBy.setLayoutData( fdDevelopedBy );
    wDevelopedBy.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        Program.launch( "http://www.inquidia.com" );
      }
    } );

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
    wResumeGroup.setVisible( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_RESUME );
    wSuspendGroup.setVisible( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_SUSPEND );
    wAlterGroup.setVisible( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_ALTER );

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
    wResumeFailIfNotExists.setSelection( jobEntry.isFailIfNotExists() );
    wSuspendFailIfNotExists.setSelection( jobEntry.isFailIfNotExists() );

    wAlterFailIfNotExists.setSelection( jobEntry.isFailIfNotExists() );
    if ( warehouseSizeId >= 0 && warehouseSizeId < WAREHOUSE_SIZE_DESCS.length ) {
      wAlterWarehouseSize.setText( WAREHOUSE_SIZE_DESCS[warehouseSizeId] );
    } else {
      wAlterWarehouseSize.setText( Const.NVL( jobEntry.getWarehouseSize(), "" ) );
    }
    if ( warehouseTypeId >= 0 && warehouseTypeId < WAREHOUSE_TYPE_DESCS.length ) {
      wAlterWarehouseType.setText( WAREHOUSE_TYPE_DESCS[warehouseTypeId] );
    } else {
      wAlterWarehouseType.setText( Const.NVL( jobEntry.getWarehouseType(), "" ) );
    }
    wAlterMaxClusterSize.setText( Const.NVL( jobEntry.getMaxClusterCount(), "" ) );
    wAlterMinClusterSize.setText( Const.NVL( jobEntry.getMinClusterCount(), "" ) );
    wAlterAutoSuspend.setText( Const.NVL( jobEntry.getAutoSuspend(), "" ) );
    wAlterAutoResume.setSelection( jobEntry.isAutoResume() );
    wAlterResourceMonitor.setText( Const.NVL( jobEntry.getResourceMonitor(), "" ) );
    wAlterComment.setText( Const.NVL( jobEntry.getComment(), "" ) );

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
    } else if ( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_RESUME ) {
      jobEntry.setFailIfNotExists( wResumeFailIfNotExists.getSelection() );
    } else if ( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_SUSPEND ) {
      jobEntry.setFailIfNotExists( wSuspendFailIfNotExists.getSelection() );
    } else if ( wAction.getSelectionIndex() == WarehouseManager.MANAGEMENT_ACTION_ALTER ) {
      jobEntry.setFailIfNotExists( wAlterFailIfNotExists.getSelection() );
      boolean warehouseSizeFound = false;
      for ( int i = 0; i < WAREHOUSE_SIZE_DESCS.length; i++ ) {
        if ( wAlterWarehouseSize.getText().equals( WAREHOUSE_SIZE_DESCS[i] ) ) {
          warehouseSizeFound = true;
          jobEntry.setWarehouseSizeById( i );
          break;
        }
      }
      if ( !warehouseSizeFound ) {
        jobEntry.setWarehouseSize( wAlterWarehouseSize.getText() );
      }

      boolean warehouseTypeFound = false;
      for ( int i = 0; i < WAREHOUSE_TYPE_DESCS.length; i++ ) {
        if ( wAlterWarehouseType.getText().equals( WAREHOUSE_TYPE_DESCS[i] ) ) {
          warehouseTypeFound = true;
          jobEntry.setWarehouseTypeById( i );
          break;
        }
      }
      if ( !warehouseTypeFound ) {
        jobEntry.setWarehouseType( wAlterWarehouseType.getText() );
      }

      jobEntry.setMaxClusterCount( wAlterMaxClusterSize.getText() );
      jobEntry.setMinClusterCount( wAlterMinClusterSize.getText() );
      jobEntry.setAutoResume( wAlterAutoResume.getSelection() );
      jobEntry.setAutoSuspend( wAlterAutoSuspend.getText() );
      jobEntry.setResourceMonitor( wAlterResourceMonitor.getText() );
      jobEntry.setComment( wAlterComment.getText() );
    }

    dispose();
  }


  public void getResourceMonitors() {
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
}
