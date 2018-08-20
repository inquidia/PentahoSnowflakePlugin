/*! ******************************************************************************
 *
 * Copyright 2016 Inquidia Consulting
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

package org.inquidia.kettle.plugins.snowflakeplugin.bulkloader;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Inquidia Consulting
 */
@SuppressWarnings( { "FieldCanBeLocal", "WeakerAccess", "unused" } )
public class SnowflakeBulkLoaderDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = SnowflakeBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  /**
   * The descriptions for the location type drop down
   */
  private static final String[] LOCATION_TYPE_COMBO = new String[] {
    BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.LocationType.User" ),
    BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.LocationType.Table" ),
    BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.LocationType.InternalStage" ) };

  /**
   * The descriptions for the on error drop down
   */
  private static final String[] ON_ERROR_COMBO = new String[] {
          BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.OnError.Continue" ),
          BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.OnError.SkipFile" ),
          BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.OnError.SkipFilePercent" ),
          BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.OnError.Abort" ) };

  /**
   * The descriptions for the data type drop down
   */
  private static final String[] DATA_TYPE_COMBO = new String[] {
          BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.DataType.CSV" ),
          BaseMessages.getString( PKG, "SnowflakeBulkLoad.Dialog.DataType.JSON" ) };

  //The tabs
  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wLoaderTab, wDataTypeTab, wFieldsTab;

  private FormData fdLoaderComp, fdDataTypeComp, fdFieldsComp;

  /* ********************************************************
   * Loader tab
   * This tab is used to configure information about the
   * database and bulk load method.
   * ********************************************************/

  // Database connection line
  private CCombo wConnection;

  // Schema line
  private Label wlSchema;
  private TextVar wSchema;
  private FormData fdlSchema, fdSchema;
  private FormData fdbSchema;
  private Button wbSchema;

  // Table line
  private Label wlTable;
  private Button wbTable;
  private TextVar wTable;
  private FormData fdlTable, fdbTable, fdTable;

  // Location Type line
  private Label wlLocationType;
  private CCombo wLocationType;
  private FormData fdlLocationType, fdLocationType;

  // Stage Name line
  private Label wlStageName;
  private ComboVar wStageName;
  private FormData fdlStageName, fdStageName;

  // Work Directory Line
  private Label wlWorkDirectory;
  private TextVar wWorkDirectory;
  private Button wbWorkDirectory;
  private FormData fdlWorkDirectory, fdWorkDirectory, fdbWorkDirectory;

  // On Error Line
  private Label wlOnError;
  private CCombo wOnError;
  private FormData fdlOnError, fdOnError;

  // Error Limit Line
  private Label wlErrorLimit;
  private TextVar wErrorLimit;
  private FormData fdlErrorLimit, fdErrorLimit;

  // Split Size Line
  private Label wlSplitSize;
  private TextVar wSplitSize;
  private FormData fdlSplitSize, fdSplitSize;

  // Remove files line
  private Label wlRemoveFiles;
  private Button wRemoveFiles;
  private FormData fdlRemoveFiles, fdRemoveFiles;

  /* *************************************************************
   * End Loader Tab
   * *************************************************************/

  /* *************************************************************
   * Begin Data Type tab
   * This tab is used to configure the specific loading parameters
   * for the data type selected.
   * *************************************************************/

  // Data Type Line
  private Label wlDataType;
  private CCombo wDataType;
  private FormData fdlDataType, fdDataType;

  /* -------------------------------------------------------------
   * CSV Group
   * ------------------------------------------------------------*/

  private Group gCsvGroup;
  private FormData fdgCsvGroup;

  // Trim Whitespace line
  private Label wlTrimWhitespace;
  private Button wTrimWhitespace;
  private FormData fdlTrimWhitespace, fdTrimWhitespace;

  // Null If line
  private Label wlNullIf;
  private TextVar wNullIf;
  private FormData fdlNullIf, fdNullIf;

  // Error on column mismatch line
  private Label wlColumnMismatch;
  private Button wColumnMismatch;
  private FormData fdlColumnMismatch, fdColumnMismatch;

  /* --------------------------------------------------
   * End CSV Group
   * -------------------------------------------------*/

  /* --------------------------------------------------
   * Start JSON Group
   * -------------------------------------------------*/

  private Group gJsonGroup;
  private FormData fdgJsonGroup;

  // Strip null line
  private Label wlStripNull;
  private Button wStripNull;
  private FormData fdlStripNull, fdStripNull;

  // Ignore UTF-8 Error line
  private Label wlIgnoreUtf8;
  private Button wIgnoreUtf8;
  private FormData fdlIgnoreUtf8, fdIgnoreUtf8;

  // Allow duplicate elements lines
  private Label wlAllowDuplicate;
  private Button wAllowDuplicate;
  private FormData fdlAllowDuplicate, fdAllowDuplicate;

  // Enable Octal line
  private Label wlEnableOctal;
  private Button wEnableOctal;
  private FormData fdlEnableOctal, fdEnableOctal;

  /* -------------------------------------------------
   * End JSON Group
   * ------------------------------------------------*/

  /* ************************************************
   * End Data tab
   * ************************************************/

  /* ************************************************
   * Start fields tab
   * This tab is used to define the field mappings
   * from the stream field to the database
   * ************************************************/

  // Specify Fields line
  private Label wlSpecifyFields;
  private Button wSpecifyFields;
  private FormData fdlSpecifyFields, fdSpecifyFields;

  // JSON Field Line
  private Label wlJsonField;
  private CCombo wJsonField;
  private FormData fdlJsonField, fdJsonField;

  // Field mapping table
  private TableView wFields;
  private FormData fdFields;
  private ColumnInfo[] colinf;

  // Enter field mapping
  private Button wDoMapping;
  private FormData fdbDoMapping;

  /* ************************************************
   * End Fields tab
   * ************************************************/

  private SnowflakeBulkLoaderMeta input;

  private Link wDevelopedBy;
  private FormData fdDevelopedBy;

  private Map<String, Integer> inputFields;

  private Display display;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  private int margin = Const.MARGIN;

  public SnowflakeBulkLoaderDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (SnowflakeBulkLoaderMeta) in;
    inputFields = new HashMap<>();
  }

  /**
   * Open the Bulk Loader dialog
   * @return The step name
   */
  public String open() {
    Shell parent = getParent();
    display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    /* ************************************************
     * Modify Listeners
     * ************************************************/

    // Basic modify listener, sets if anything has changed.  Pentaho's way to know the transformation
    // needs saved
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };

    SelectionAdapter bMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    };

    // Some settings have to modify what is or is not visible within the shell.  This listener does this.
    SelectionAdapter lsFlags = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setFlags();
      }
    };

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.Title" ) );

    int middle = props.getMiddlePct();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    /* *********************************************
     * Start of Loader tab
     * *********************************************/

    wLoaderTab = new CTabItem( wTabFolder, SWT.NONE );
    wLoaderTab.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.LoaderTab.TabTitle" ) );

    Composite wLoaderComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wLoaderComp );

    FormLayout loaderLayout = new FormLayout();
    loaderLayout.marginWidth = 3;
    loaderLayout.marginHeight = 3;
    wLoaderComp.setLayout( loaderLayout );

    // Connection line
    wConnection = addConnectionLine( wLoaderComp, wStepname, middle, margin );
    if ( input.getDatabaseMeta() == null && transMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );

    // Schema line
    wlSchema = new Label( wLoaderComp, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.Schema.Label" ) );
    wlSchema.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.Schema.Tooltip" ) );
    props.setLook( wlSchema );
    fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdlSchema.right = new FormAttachment( middle, -margin );
    wlSchema.setLayoutData( fdlSchema );

    wbSchema = new Button( wLoaderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbSchema );
    wbSchema.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment( wConnection, 2 * margin );
    fdbSchema.right = new FormAttachment( 100, 0 );
    wbSchema.setLayoutData( fdbSchema );

    wSchema = new TextVar( transMeta, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsMod );
    fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment( wbSchema, -margin );
    wSchema.setLayoutData( fdSchema );
    wSchema.addFocusListener( new FocusAdapter() {
      @Override
      public void focusLost( FocusEvent focusEvent ) {
        setTableFieldCombo();
      }
    } );

    // Table line...
    wlTable = new Label( wLoaderComp, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.Table.Label" ) );
    props.setLook( wlTable );
    fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wbSchema, margin );
    wlTable.setLayoutData( fdlTable );

    wbTable = new Button( wLoaderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wbSchema, margin );
    wbTable.setLayoutData( fdbTable );

    wTable = new TextVar( transMeta, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsMod );
    fdTable = new FormData();
    fdTable.top = new FormAttachment( wbSchema, margin );
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData( fdTable );
    wTable.addFocusListener( new FocusAdapter() {
      @Override
      public void focusLost( FocusEvent focusEvent ) {
        setTableFieldCombo();
      }
    } );

    // Location Type line
    //
    wlLocationType = new Label( wLoaderComp, SWT.RIGHT );
    wlLocationType.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.LocationType.Label" ) );
    wlLocationType.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.LocationType.Tooltip" ) );
    props.setLook( wlLocationType );
    fdlLocationType = new FormData();
    fdlLocationType.left = new FormAttachment( 0, 0 );
    fdlLocationType.top = new FormAttachment( wTable, margin * 2 );
    fdlLocationType.right = new FormAttachment( middle, -margin );
    wlLocationType.setLayoutData( fdlLocationType );

    wLocationType = new CCombo( wLoaderComp, SWT.BORDER | SWT.READ_ONLY );
    wLocationType.setEditable( false );
    props.setLook( wLocationType );
    wLocationType.addModifyListener( lsMod );
    wLocationType.addSelectionListener( lsFlags );
    fdLocationType = new FormData();
    fdLocationType.left = new FormAttachment( middle, 0 );
    fdLocationType.top = new FormAttachment( wTable, margin * 2 );
    fdLocationType.right = new FormAttachment( 100, 0 );
    wLocationType.setLayoutData( fdLocationType );
    for ( String locationType : LOCATION_TYPE_COMBO ) {
      wLocationType.add( locationType );
    }

    // Stage name line
    //
    wlStageName = new Label( wLoaderComp, SWT.RIGHT );
    wlStageName.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.StageName.Label" ) );
    props.setLook( wlStageName );
    fdlStageName = new FormData();
    fdlStageName.left = new FormAttachment( 0, 0 );
    fdlStageName.top = new FormAttachment( wLocationType, margin * 2 );
    fdlStageName.right = new FormAttachment( middle, -margin );
    wlStageName.setLayoutData( fdlStageName );

    wStageName = new ComboVar( transMeta, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStageName );
    wStageName.addModifyListener( lsMod );
    wStageName.addSelectionListener( lsFlags );
    fdStageName = new FormData();
    fdStageName.left = new FormAttachment( middle, 0 );
    fdStageName.top = new FormAttachment( wLocationType, margin * 2 );
    fdStageName.right = new FormAttachment( 100, 0 );
    wStageName.setLayoutData( fdStageName );
    wStageName.setEnabled( false );
    wStageName.addFocusListener( new FocusAdapter() {
      /**
       * Get the list of stages for the schema, and populate the stage name drop down.
       * @param focusEvent The event
       */
      @Override
      public void focusGained( FocusEvent focusEvent ) {
        String stageNameText = wStageName.getText();
        wStageName.removeAll();

        DatabaseMeta databaseMeta = transMeta.findDatabase( wConnection.getText() );
        if ( databaseMeta != null ) {
          Database db = new Database( loggingObject, databaseMeta );
          try {
            db.connect();
            String SQL = "show stages";
            if ( !Const.isEmpty( transMeta.environmentSubstitute( wSchema.getText() ) ) ) {
              SQL += " in " + transMeta.environmentSubstitute( wSchema.getText() );
            }

            ResultSet resultSet = db.openQuery( SQL, null, null, ResultSet.FETCH_FORWARD, false );
            RowMetaInterface rowMeta = db.getReturnRowMeta();
            Object[] row = db.getRow( resultSet );
            int nameField = rowMeta.indexOfValue( "NAME" );
            if ( nameField >= 0 ) {
              while ( row != null ) {
                String stageName = rowMeta.getString( row, nameField );
                wStageName.add( stageName );
                row = db.getRow( resultSet );
              }
            } else {
              throw new KettleException( "Unable to find stage name field in result" );
            }
            db.closeQuery( resultSet );
            if ( stageNameText != null ) {
              wStageName.setText( stageNameText );
            }


          } catch ( Exception ex ) {
            logDebug( "Error getting stages", ex );
          } finally {
            try {

              db.disconnect();
            } catch ( Exception ex ) {
              // Nothing more we can do
            }
          }
        }
      }
    } );

    // Work directory line
    wlWorkDirectory = new Label( wLoaderComp, SWT.RIGHT );
    wlWorkDirectory.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.WorkDirectory.Label" ) );
    wlWorkDirectory.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.WorkDirectory.Tooltip" ) );
    props.setLook( wlWorkDirectory );
    fdlWorkDirectory = new FormData();
    fdlWorkDirectory.left = new FormAttachment( 0, 0 );
    fdlWorkDirectory.top = new FormAttachment( wStageName, margin );
    fdlWorkDirectory.right = new FormAttachment( middle, -margin );
    wlWorkDirectory.setLayoutData( fdlWorkDirectory );

    wbWorkDirectory = new Button( wLoaderComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbWorkDirectory );
    wbWorkDirectory.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbWorkDirectory = new FormData();
    fdbWorkDirectory.right = new FormAttachment( 100, 0 );
    fdbWorkDirectory.top = new FormAttachment( wStageName, margin );
    wbWorkDirectory.setLayoutData( fdbWorkDirectory );

    wWorkDirectory = new TextVar( transMeta, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWorkDirectory.setText( "temp" );
    props.setLook( wWorkDirectory );
    wWorkDirectory.addModifyListener( lsMod );
    fdWorkDirectory = new FormData();
    fdWorkDirectory.left = new FormAttachment( middle, 0 );
    fdWorkDirectory.top = new FormAttachment( wStageName, margin );
    fdWorkDirectory.right = new FormAttachment( wbWorkDirectory, -margin );
    wWorkDirectory.setLayoutData( fdWorkDirectory );

    wbWorkDirectory.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        DirectoryDialog dd = new DirectoryDialog( shell, SWT.NONE );
        dd.setFilterPath( wWorkDirectory.getText() );
        String dir = dd.open();
        if ( dir != null ) {
          wWorkDirectory.setText( dir );
        }
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version:
    wWorkDirectory.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wWorkDirectory.setToolTipText( transMeta.environmentSubstitute( wWorkDirectory.getText() ) );
      }
    } );

    // On Error line
    //
    wlOnError = new Label( wLoaderComp, SWT.RIGHT );
    wlOnError.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.OnError.Label" ) );
    wlOnError.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.OnError.Tooltip" ) );
    props.setLook( wlOnError );
    fdlOnError = new FormData();
    fdlOnError.left = new FormAttachment( 0, 0 );
    fdlOnError.top = new FormAttachment( wWorkDirectory, margin * 2 );
    fdlOnError.right = new FormAttachment( middle, -margin );
    wlOnError.setLayoutData( fdlOnError );

    wOnError = new CCombo( wLoaderComp, SWT.BORDER | SWT.READ_ONLY );
    wOnError.setEditable( false );
    props.setLook( wOnError );
    wOnError.addModifyListener( lsMod );
    wOnError.addSelectionListener( lsFlags );
    fdOnError = new FormData();
    fdOnError.left = new FormAttachment( middle, 0 );
    fdOnError.top = new FormAttachment( wWorkDirectory, margin * 2 );
    fdOnError.right = new FormAttachment( 100, 0 );
    wOnError.setLayoutData( fdOnError );
    for ( String onError : ON_ERROR_COMBO ) {
      wOnError.add( onError );
    }

    // Error Limit line
    wlErrorLimit = new Label( wLoaderComp, SWT.RIGHT );
    wlErrorLimit.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Label" ) );
    wlErrorLimit.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Tooltip" ) );
    props.setLook( wlErrorLimit );
    fdlErrorLimit = new FormData();
    fdlErrorLimit.left = new FormAttachment( 0, 0 );
    fdlErrorLimit.top = new FormAttachment( wOnError, margin );
    fdlErrorLimit.right = new FormAttachment( middle, -margin );
    wlErrorLimit.setLayoutData( fdlErrorLimit );


    wErrorLimit = new TextVar( transMeta, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorLimit );
    wErrorLimit.addModifyListener( lsMod );
    fdErrorLimit = new FormData();
    fdErrorLimit.left = new FormAttachment( middle, 0 );
    fdErrorLimit.top = new FormAttachment( wOnError, margin );
    fdErrorLimit.right = new FormAttachment( 100, 0 );
    wErrorLimit.setLayoutData( fdErrorLimit );

    //Size limit line
    wlSplitSize = new Label( wLoaderComp, SWT.RIGHT );
    wlSplitSize.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.SplitSize.Label" ) );
    wlSplitSize.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.SplitSize.Tooltip" ) );
    props.setLook( wlSplitSize );
    fdlSplitSize = new FormData();
    fdlSplitSize.left = new FormAttachment( 0, 0 );
    fdlSplitSize.top = new FormAttachment( wErrorLimit, margin );
    fdlSplitSize.right = new FormAttachment( middle, -margin );
    wlSplitSize.setLayoutData( fdlSplitSize );

    wSplitSize = new TextVar( transMeta, wLoaderComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSplitSize );
    wSplitSize.addModifyListener( lsMod );
    fdSplitSize = new FormData();
    fdSplitSize.left = new FormAttachment( middle, 0 );
    fdSplitSize.top = new FormAttachment( wErrorLimit, margin );
    fdSplitSize.right = new FormAttachment( 100, 0 );
    wSplitSize.setLayoutData( fdSplitSize );

    // Remove files line
    //
    wlRemoveFiles = new Label( wLoaderComp, SWT.RIGHT );
    wlRemoveFiles.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.RemoveFiles.Label" ) );
    wlRemoveFiles.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.RemoveFiles.Tooltip" ) );
    props.setLook( wlRemoveFiles );
    fdlRemoveFiles = new FormData();
    fdlRemoveFiles.left = new FormAttachment( 0, 0 );
    fdlRemoveFiles.top = new FormAttachment( wSplitSize, margin );
    fdlRemoveFiles.right = new FormAttachment( middle, -margin );
    wlRemoveFiles.setLayoutData( fdlRemoveFiles );

    wRemoveFiles = new Button( wLoaderComp, SWT.CHECK );
    props.setLook( wRemoveFiles );
    fdRemoveFiles = new FormData();
    fdRemoveFiles.left = new FormAttachment( middle, 0 );
    fdRemoveFiles.top = new FormAttachment( wSplitSize, margin );
    fdRemoveFiles.right = new FormAttachment( 100, 0 );
    wRemoveFiles.setLayoutData( fdRemoveFiles );
    wRemoveFiles.addSelectionListener( bMod );

    fdLoaderComp = new FormData();
    fdLoaderComp.left = new FormAttachment( 0, 0 );
    fdLoaderComp.top = new FormAttachment( 0, 0 );
    fdLoaderComp.right = new FormAttachment( 100, 0 );
    fdLoaderComp.bottom = new FormAttachment( 100, 0 );
    wLoaderComp.setLayoutData( fdLoaderComp );

    wLoaderComp.layout();
    wLoaderTab.setControl( wLoaderComp );

    /* ********************************************************
     * End Loader tab
     * ********************************************************/

    /* ********************************************************
     * Start data type tab
     * ********************************************************/

    wDataTypeTab = new CTabItem( wTabFolder, SWT.NONE );
    wDataTypeTab.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.DataTypeTab.TabTitle" ) );

    Composite wDataTypeComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wDataTypeComp );

    FormLayout dataTypeLayout = new FormLayout();
    dataTypeLayout.marginWidth = 3;
    dataTypeLayout.marginHeight = 3;
    wDataTypeComp.setLayout( dataTypeLayout );

    // Data Type Line
    //
    wlDataType = new Label( wDataTypeComp, SWT.RIGHT );
    wlDataType.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.DataType.Label" ) );
    props.setLook( wlDataType );
    fdlDataType = new FormData();
    fdlDataType.left = new FormAttachment( 0, 0 );
    fdlDataType.top = new FormAttachment( 0, margin );
    fdlDataType.right = new FormAttachment( middle, -margin );
    wlDataType.setLayoutData( fdlDataType );

    wDataType = new CCombo( wDataTypeComp, SWT.BORDER | SWT.READ_ONLY );
    wDataType.setEditable( false );
    props.setLook( wDataType );
    wDataType.addModifyListener( lsMod );
    wDataType.addSelectionListener( lsFlags );
    fdDataType = new FormData();
    fdDataType.left = new FormAttachment( middle, 0 );
    fdDataType.top = new FormAttachment( 0, margin );
    fdDataType.right = new FormAttachment( 100, 0 );
    wDataType.setLayoutData( fdDataType );
    for ( String dataType : DATA_TYPE_COMBO ) {
      wDataType.add( dataType );
    }

    /////////////////////
    // Start CSV Group
    /////////////////////
    gCsvGroup = new Group( wDataTypeComp, SWT.SHADOW_ETCHED_IN );
    gCsvGroup.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.CSVGroup.Label" ) );
    FormLayout csvLayout = new FormLayout();
    csvLayout.marginWidth = 3;
    csvLayout.marginHeight = 3;
    gCsvGroup.setLayout( csvLayout );
    props.setLook( gCsvGroup );

    fdgCsvGroup = new FormData();
    fdgCsvGroup.left = new FormAttachment( 0, 0 );
    fdgCsvGroup.right = new FormAttachment( 100, 0 );
    fdgCsvGroup.top = new FormAttachment( wDataType, margin * 2 );
    fdgCsvGroup.bottom = new FormAttachment( 100, -margin * 2 );
    gCsvGroup.setLayoutData( fdgCsvGroup );

    // Trim Whitespace line
    wlTrimWhitespace = new Label( gCsvGroup, SWT.RIGHT );
    wlTrimWhitespace.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.TrimWhitespace.Label" ) );
    wlTrimWhitespace.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.TrimWhitespace.Tooltip" ) );
    props.setLook( wlTrimWhitespace );
    fdlTrimWhitespace = new FormData();
    fdlTrimWhitespace.left = new FormAttachment( 0, 0 );
    fdlTrimWhitespace.top = new FormAttachment( 0, margin );
    fdlTrimWhitespace.right = new FormAttachment( middle, -margin );
    wlTrimWhitespace.setLayoutData( fdlTrimWhitespace );

    wTrimWhitespace = new Button( gCsvGroup, SWT.CHECK );
    props.setLook( wTrimWhitespace );
    fdTrimWhitespace = new FormData();
    fdTrimWhitespace.left = new FormAttachment( middle, 0 );
    fdTrimWhitespace.top = new FormAttachment( 0, margin );
    fdTrimWhitespace.right = new FormAttachment( 100, 0 );
    wTrimWhitespace.setLayoutData( fdTrimWhitespace );
    wTrimWhitespace.addSelectionListener( bMod );

    // Null if line
    wlNullIf = new Label( gCsvGroup, SWT.RIGHT );
    wlNullIf.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.NullIf.Label" ) );
    wlNullIf.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.NullIf.Tooltip" ) );
    props.setLook( wlNullIf );
    fdlNullIf = new FormData();
    fdlNullIf.left = new FormAttachment( 0, 0 );
    fdlNullIf.top = new FormAttachment( wTrimWhitespace, margin );
    fdlNullIf.right = new FormAttachment( middle, -margin );
    wlNullIf.setLayoutData( fdlNullIf );

    wNullIf = new TextVar( transMeta, gCsvGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNullIf );
    wNullIf.addModifyListener( lsMod );
    fdNullIf = new FormData();
    fdNullIf.left = new FormAttachment( middle, 0 );
    fdNullIf.top = new FormAttachment( wTrimWhitespace, margin );
    fdNullIf.right = new FormAttachment( 100, 0 );
    wNullIf.setLayoutData( fdNullIf );

    // Error mismatch line
    wlColumnMismatch = new Label( gCsvGroup, SWT.RIGHT );
    wlColumnMismatch.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ColumnMismatch.Label" ) );
    wlColumnMismatch.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ColumnMismatch.Tooltip" ) );
    props.setLook( wlColumnMismatch );
    fdlColumnMismatch = new FormData();
    fdlColumnMismatch.left = new FormAttachment( 0, 0 );
    fdlColumnMismatch.top = new FormAttachment( wNullIf, margin );
    fdlColumnMismatch.right = new FormAttachment( middle, -margin );
    wlColumnMismatch.setLayoutData( fdlColumnMismatch );

    wColumnMismatch = new Button( gCsvGroup, SWT.CHECK );
    props.setLook( wColumnMismatch );
    fdColumnMismatch = new FormData();
    fdColumnMismatch.left = new FormAttachment( middle, 0 );
    fdColumnMismatch.top = new FormAttachment( wNullIf, margin );
    fdColumnMismatch.right = new FormAttachment( 100, 0 );
    wColumnMismatch.setLayoutData( fdColumnMismatch );
    wColumnMismatch.addSelectionListener( bMod );

    ///////////////////////////
    // End CSV Group
    ///////////////////////////

    ///////////////////////////
    // Start JSON Group
    ///////////////////////////
    gJsonGroup = new Group( wDataTypeComp, SWT.SHADOW_ETCHED_IN );
    gJsonGroup.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.JsonGroup.Label" ) );
    FormLayout jsonLayout = new FormLayout();
    jsonLayout.marginWidth = 3;
    jsonLayout.marginHeight = 3;
    gJsonGroup.setLayout( jsonLayout );
    props.setLook( gJsonGroup );

    fdgJsonGroup = new FormData();
    fdgJsonGroup.left = new FormAttachment( 0, 0 );
    fdgJsonGroup.right = new FormAttachment( 100, 0 );
    fdgJsonGroup.top = new FormAttachment( wDataType, margin * 2 );
    fdgJsonGroup.bottom = new FormAttachment( 100, -margin * 2 );
    gJsonGroup.setLayoutData( fdgJsonGroup );

    // Strip Null line
    wlStripNull = new Label( gJsonGroup, SWT.RIGHT );
    wlStripNull.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.StripNull.Label" ) );
    wlStripNull.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.StripNull.Tooltip" ) );
    props.setLook( wlStripNull );
    fdlStripNull = new FormData();
    fdlStripNull.left = new FormAttachment( 0, 0 );
    fdlStripNull.top = new FormAttachment( 0, margin );
    fdlStripNull.right = new FormAttachment( middle, -margin );
    wlStripNull.setLayoutData( fdlStripNull );

    wStripNull = new Button( gJsonGroup, SWT.CHECK );
    props.setLook( wStripNull );
    fdStripNull = new FormData();
    fdStripNull.left = new FormAttachment( middle, 0 );
    fdStripNull.top = new FormAttachment( 0, margin );
    fdStripNull.right = new FormAttachment( 100, 0 );
    wStripNull.setLayoutData( fdStripNull );
    wStripNull.addSelectionListener( bMod );

    // Ignore UTF8 line
    wlIgnoreUtf8 = new Label( gJsonGroup, SWT.RIGHT );
    wlIgnoreUtf8.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.IgnoreUtf8.Label" ) );
    wlIgnoreUtf8.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.IgnoreUtf8.Tooltip" ) );
    props.setLook( wlIgnoreUtf8 );
    fdlIgnoreUtf8 = new FormData();
    fdlIgnoreUtf8.left = new FormAttachment( 0, 0 );
    fdlIgnoreUtf8.top = new FormAttachment( wStripNull, margin );
    fdlIgnoreUtf8.right = new FormAttachment( middle, -margin );
    wlIgnoreUtf8.setLayoutData( fdlIgnoreUtf8 );

    wIgnoreUtf8 = new Button( gJsonGroup, SWT.CHECK );
    props.setLook( wIgnoreUtf8 );
    fdIgnoreUtf8 = new FormData();
    fdIgnoreUtf8.left = new FormAttachment( middle, 0 );
    fdIgnoreUtf8.top = new FormAttachment( wStripNull, margin );
    fdIgnoreUtf8.right = new FormAttachment( 100, 0 );
    wIgnoreUtf8.setLayoutData( fdIgnoreUtf8 );
    wIgnoreUtf8.addSelectionListener( bMod );

    // Allow duplicate elements line
    wlAllowDuplicate = new Label( gJsonGroup, SWT.RIGHT );
    wlAllowDuplicate.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.AllowDuplicate.Label" ) );
    wlAllowDuplicate.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.AllowDuplicate.Tooltip" ) );
    props.setLook( wlAllowDuplicate );
    fdlAllowDuplicate = new FormData();
    fdlAllowDuplicate.left = new FormAttachment( 0, 0 );
    fdlAllowDuplicate.top = new FormAttachment( wIgnoreUtf8, margin );
    fdlAllowDuplicate.right = new FormAttachment( middle, -margin );
    wlAllowDuplicate.setLayoutData( fdlAllowDuplicate );

    wAllowDuplicate = new Button( gJsonGroup, SWT.CHECK );
    props.setLook( wAllowDuplicate );
    fdAllowDuplicate = new FormData();
    fdAllowDuplicate.left = new FormAttachment( middle, 0 );
    fdAllowDuplicate.top = new FormAttachment( wIgnoreUtf8, margin );
    fdAllowDuplicate.right = new FormAttachment( 100, 0 );
    wAllowDuplicate.setLayoutData( fdAllowDuplicate );
    wAllowDuplicate.addSelectionListener( bMod );

    // Enable Octal line
    wlEnableOctal = new Label( gJsonGroup, SWT.RIGHT );
    wlEnableOctal.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.EnableOctal.Label" ) );
    wlEnableOctal.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.EnableOctal.Tooltip" ) );
    props.setLook( wlEnableOctal );
    fdlEnableOctal = new FormData();
    fdlEnableOctal.left = new FormAttachment( 0, 0 );
    fdlEnableOctal.top = new FormAttachment( wAllowDuplicate, margin );
    fdlEnableOctal.right = new FormAttachment( middle, -margin );
    wlEnableOctal.setLayoutData( fdlEnableOctal );

    wEnableOctal = new Button( gJsonGroup, SWT.CHECK );
    props.setLook( wEnableOctal );
    fdEnableOctal = new FormData();
    fdEnableOctal.left = new FormAttachment( middle, 0 );
    fdEnableOctal.top = new FormAttachment( wAllowDuplicate, margin );
    fdEnableOctal.right = new FormAttachment( 100, 0 );
    wEnableOctal.setLayoutData( fdEnableOctal );
    wEnableOctal.addSelectionListener( bMod );

    ////////////////////////
    // End JSON Group
    ////////////////////////

    fdDataTypeComp = new FormData();
    fdDataTypeComp.left = new FormAttachment( 0, 0 );
    fdDataTypeComp.top = new FormAttachment( 0, 0 );
    fdDataTypeComp.right = new FormAttachment( 100, 0 );
    fdDataTypeComp.bottom = new FormAttachment( 100, 0 );
    wDataTypeComp.setLayoutData( fdFieldsComp );

    wDataTypeComp.layout();
    wDataTypeTab.setControl( wDataTypeComp );

    /* ******************************************
     * End Data type tab
     * ******************************************/

    /* ******************************************
     * Start Fields tab
     * This tab is used to specify the field mapping
     * to the Snowflake table
     * ******************************************/

    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.FieldsTab.TabTitle" ) );

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout( fieldsLayout );

    // Specify Fields line
    wlSpecifyFields = new Label( wFieldsComp, SWT.RIGHT );
    wlSpecifyFields.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.SpecifyFields.Label" ) );
    wlSpecifyFields.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.SpecifyFields.Tooltip" ) );
    props.setLook( wlSpecifyFields );
    fdlSpecifyFields = new FormData();
    fdlSpecifyFields.left = new FormAttachment( 0, 0 );
    fdlSpecifyFields.top = new FormAttachment( 0, margin );
    fdlSpecifyFields.right = new FormAttachment( middle, -margin );
    wlSpecifyFields.setLayoutData( fdlSpecifyFields );

    wSpecifyFields = new Button( wFieldsComp, SWT.CHECK );
    props.setLook( wSpecifyFields );
    fdSpecifyFields = new FormData();
    fdSpecifyFields.left = new FormAttachment( middle, 0 );
    fdSpecifyFields.top = new FormAttachment( 0, margin );
    fdSpecifyFields.right = new FormAttachment( 100, 0 );
    wSpecifyFields.setLayoutData( fdSpecifyFields );
    wSpecifyFields.addSelectionListener( bMod );
    wSpecifyFields.addSelectionListener(
      new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent selectionEvent ) {
          setFlags();
        }
      }
    );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );
    fdGet = new FormData();
    fdGet.right = new FormAttachment( 50, -margin );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    wDoMapping = new Button( wFieldsComp, SWT.PUSH );
    wDoMapping.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.DoMapping.Label" ) );
    wDoMapping.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.DoMapping.Tooltip" ) );
    fdbDoMapping = new FormData();
    fdbDoMapping.left = new FormAttachment( 50, margin );
    fdbDoMapping.bottom = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData( fdbDoMapping );
    wDoMapping.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        generateMappings();
      }
    } );

    final int FieldsCols = 2;
    final int FieldsRows = input.getSnowflakeBulkLoaderFields().length;

    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.StreamField.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    colinf[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.TableField.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    tableFieldColumns.add( colinf[1] );

    wFields =
      new TableView(
        transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wSpecifyFields, margin * 3 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    // JSON Field Line
    //
    wlJsonField = new Label( wFieldsComp, SWT.RIGHT );
    wlJsonField.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.JsonField.Label" ) );
    props.setLook( wlJsonField );
    fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment( 0, 0 );
    fdlJsonField.top = new FormAttachment( 0, margin );
    fdlJsonField.right = new FormAttachment( middle, -margin );
    wlJsonField.setLayoutData( fdlJsonField );

    wJsonField = new CCombo( wFieldsComp, SWT.BORDER | SWT.READ_ONLY );
    wJsonField.setEditable( false );
    props.setLook( wJsonField );
    wJsonField.addModifyListener( lsMod );
    fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment( middle, 0 );
    fdJsonField.top = new FormAttachment( 0, margin );
    fdJsonField.right = new FormAttachment( 100, 0 );
    wJsonField.setLayoutData( fdJsonField );
    wJsonField.addFocusListener( new FocusAdapter() {
      /**
       * Get the fields from the previous step and populate the JSON Field drop down
       * @param focusEvent The event
       */
      @Override
      public void focusGained( FocusEvent focusEvent ) {
        try {
          RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );
          String jsonField = wJsonField.getText();
          wJsonField.setItems( row.getFieldNames() );
          if ( jsonField != null ) {
            wJsonField.setText( jsonField );
          }
        } catch ( Exception ex ) {
          String jsonField = wJsonField.getText();
          wJsonField.setItems( new String[] {} );
          wJsonField.setText( jsonField );
        }
      }
    } );

    //
    // Search the fields in the background and populate the CSV Field mapping table's stream field column
    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), i );
            }
            setComboBoxes();
          } catch ( KettleException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wTabFolder );

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

    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );
    wbSchema.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getSchemaNames();
      }
    } );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );

   // Whenever something changes, set the tooltip to the expanded version:
    wSchema.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wSchema.setToolTipText( transMeta.environmentSubstitute( wSchema.getText() ) );
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();

    setTableFieldCombo();
    setFlags();

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Sets the input stream field names in the JSON field drop down, and the Stream field drop down in the field
   * mapping table.
   */
  private void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    colinf[0].setComboValues( fieldNames );
  }



  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  private void getData() {
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    if ( input.getTargetSchema() != null ) {
      wSchema.setText( input.getTargetSchema() );
    }

    if ( input.getTargetTable() != null ) {
      wTable.setText( input.getTargetTable() );
    }

    if ( input.getLocationType() != null ) {
      wLocationType.setText( LOCATION_TYPE_COMBO[input.getLocationTypeId()] );
    }

    if ( input.getStageName() != null ) {
      wStageName.setText( input.getStageName() );
    }

    if ( input.getWorkDirectory() != null ) {
      wWorkDirectory.setText( input.getWorkDirectory() );
    }

    if ( input.getOnError() != null ) {
      wOnError.setText( ON_ERROR_COMBO[input.getOnErrorId()] );
    }

    if ( input.getErrorLimit() != null ) {
      wErrorLimit.setText( input.getErrorLimit() );
    }

    if ( input.getSplitSize() != null ) {
      wSplitSize.setText( input.getSplitSize() );
    }

    wRemoveFiles.setSelection( input.isRemoveFiles() );

    if ( input.getDataType() != null ) {
      wDataType.setText( DATA_TYPE_COMBO[input.getDataTypeId()] );
    }

    wTrimWhitespace.setSelection( input.isTrimWhitespace() );

    if ( input.getNullIf() != null ) {
      wNullIf.setText( input.getNullIf() );
    }

    wColumnMismatch.setSelection( input.isErrorColumnMismatch() );

    wStripNull.setSelection( input.isStripNull() );

    wIgnoreUtf8.setSelection( input.isIgnoreUtf8() );

    wAllowDuplicate.setSelection( input.isAllowDuplicateElements() );

    wEnableOctal.setSelection( input.isEnableOctal() );

    wSpecifyFields.setSelection( input.isSpecifyFields() );

    if ( input.getJsonField() != null ) {
      wJsonField.setText( input.getJsonField() );
    }

    logDebug( "getting fields info..." );

    for ( int i = 0; i < input.getSnowflakeBulkLoaderFields().length; i++ ) {
      SnowflakeBulkLoaderField field = input.getSnowflakeBulkLoaderFields()[i];

      TableItem item = wFields.table.getItem( i );
      item.setText( 1, Const.NVL( field.getStreamField(), "" ) );
      item.setText( 2, Const.NVL( field.getTableField(), "" ) );

    }

    wFields.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  /**
   * Cancel making changes.  Do not save any of the changes and do not set the transformation as changed.
   */
  private void cancel() {
    stepname = null;

    input.setChanged( backupChanged );

    dispose();
  }

  /**
   * Save the step settings to the step metadata
   * @param sbl The step metadata
   */
  private void saveInfo( SnowflakeBulkLoaderMeta sbl ) {
    sbl.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
    sbl.setTargetSchema( wSchema.getText() );
    sbl.setTargetTable( wTable.getText() );
    sbl.setLocationTypeById( wLocationType.getSelectionIndex() );
    sbl.setStageName( wStageName.getText() );
    sbl.setWorkDirectory( wWorkDirectory.getText() );
    sbl.setOnErrorById( wOnError.getSelectionIndex() );
    sbl.setErrorLimit( wErrorLimit.getText() );
    sbl.setSplitSize( wSplitSize.getText() );
    sbl.setRemoveFiles( wRemoveFiles.getSelection() );

    sbl.setDataTypeById( wDataType.getSelectionIndex() );
    sbl.setTrimWhitespace( wTrimWhitespace.getSelection() );
    sbl.setNullIf( wNullIf.getText() );
    sbl.setErrorColumnMismatch( wColumnMismatch.getSelection() );
    sbl.setStripNull( wStripNull.getSelection() );
    sbl.setIgnoreUtf8( wIgnoreUtf8.getSelection() );
    sbl.setAllowDuplicateElements( wAllowDuplicate.getSelection() );
    sbl.setEnableOctal( wEnableOctal.getSelection() );

    sbl.setSpecifyFields( wSpecifyFields.getSelection() );
    sbl.setJsonField( wJsonField.getText() );

    // Table table = wFields.table;

    int nrfields = wFields.nrNonEmpty();

    sbl.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      SnowflakeBulkLoaderField field = new SnowflakeBulkLoaderField();

      TableItem item = wFields.getNonEmpty( i );
      field.setStreamField( item.getText( 1 ) );
      field.setTableField( item.getText( 2 ) );
      sbl.getSnowflakeBulkLoaderFields()[i] = field;
    }
  }

  /**
   * Save the step settings and close the dialog
   */
  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    saveInfo( input );

    dispose();
  }

  /**
   * Get the fields from the previous step and load the field mapping table with a direct mapping of input fields to
   * table fields.
   */
  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog(
              shell, BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.FailedToGetFields.DialogTitle" ), BaseMessages
              .getString( PKG, "SnowflakeBulkLoader.Dialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  /**
   * Reads in the fields from the previous steps and from the ONE next step and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    RowMetaInterface sourceFields;
    RowMetaInterface targetFields;

    try {
      sourceFields = transMeta.getPrevStepFields( stepMeta );
    } catch ( KettleException e ) {
      new ErrorDialog( shell,
              BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.DoMapping.UnableToFindSourceFields.Title" ),
              BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }

    // refresh data
    input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
    input.setTargetTable( transMeta.environmentSubstitute( wTable.getText() ) );
    input.setTargetSchema( transMeta.environmentSubstitute( wSchema.getText() ) );
    StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
    try {
      targetFields = stepMetaInterface.getRequiredFields( transMeta );
    } catch ( KettleException e ) {
      new ErrorDialog( shell,
              BaseMessages.getString( PKG, "SnowflakeBulkLoader.DoMapping.UnableToFindTargetFields.Title" ),
              BaseMessages.getString( PKG, "SnowflakeBulkLoader.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      String source = item.getText( 1 );
      String target = item.getText( 2 );

      int sourceIndex = sourceFields.indexOfValue( source );
      if ( sourceIndex < 0 ) {
        missingSourceFields.append( Const.CR ).append( "   " ).append( source ).append( " --> " ).append( target );
      }
      int targetIndex = targetFields.indexOfValue( target );
      if ( targetIndex < 0 ) {
        missingTargetFields.append( Const.CR ).append( "   " ).append( source ).append( " --> " ).append( target );
      }
      if ( sourceIndex < 0 || targetIndex < 0 ) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping( sourceIndex, targetIndex );
      mappings.add( mapping );
    }

    // show a confirm dialog if some missing field was found
    //
    if ( missingSourceFields.length() > 0 || missingTargetFields.length() > 0 ) {

      String message = "";
      if ( missingSourceFields.length() > 0 ) {
        message += BaseMessages.getString( PKG, "SnowflakeBulkLoader.DoMapping.SomeSourceFieldsNotFound",
                missingSourceFields.toString() ) + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message += BaseMessages.getString( PKG, "SnowflakeBulkLoader.DoMapping.SomeTargetFieldsNotFound",
                missingTargetFields.toString() ) + Const.CR;
      }
      message += Const.CR;
      message +=
              BaseMessages.getString( PKG, "SnowflakeBulkLoader.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
      boolean goOn =
              MessageDialog.openConfirm( shell, BaseMessages.getString(
                      PKG, "SnowflakeBulkLoader.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
            new EnterMappingDialog( SnowflakeBulkLoaderDialog.this.shell, sourceFields.getFieldNames(), targetFields
                    .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      wFields.table.removeAll();
      wFields.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = mappings.get( i );
        TableItem item = wFields.table.getItem( i );
        item.setText( 1, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 2, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
  }

  /**
   * Presents a dialog box to select a schema from the database.  Then sets the selected schema in the dialog
   */
  private void getSchemaNames() {
    DatabaseMeta databaseMeta = transMeta.findDatabase( wConnection.getText() );
    if ( databaseMeta != null ) {
      Database database = new Database( loggingObject, databaseMeta );
      try {
        database.connect();
        String[] schemas = database.getSchemas();

        if ( null != schemas && schemas.length > 0 ) {
          schemas = Const.sortStrings( schemas );
          EnterSelectionDialog dialog =
                  new EnterSelectionDialog( shell, schemas, BaseMessages.getString(
                          PKG, "SnowflakeBulkLoader.Dialog.AvailableSchemas.Title", wConnection.getText() ), BaseMessages
                          .getString( PKG, "SnowflakeBulkLoader.Dialog.AvailableSchemas.Message", wConnection.getText() ) );
          String d = dialog.open();
          if ( d != null ) {
            wSchema.setText( Const.NVL( d, "" ) );
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.NoSchema.Error" ) );
          mb.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.GetSchemas.Error" ) );
          mb.open();
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.Error.Title" ), BaseMessages
                .getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorGettingSchemas" ), e );
      } finally {
        database.disconnect();
      }
    }
  }

  /**
   * Opens a dialog to select a table name
   */
  private void getTableName() {
    // New class: SelectTableDialog
    int connr = wConnection.getSelectionIndex();
    if ( connr >= 0 ) {
      DatabaseMeta inf = transMeta.getDatabase( connr );

      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog..Log.LookingAtConnection", inf.toString() ) );
      }

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, inf, transMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }

  }

  /**
   * Sets the values for the combo box in the table field on the fields tab
   */
  private void setTableFieldCombo() {
    Runnable fieldLoader = new Runnable() {
      public void run() {
        if ( !wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed() ) {
          final String tableName = wTable.getText(), connectionName = wConnection.getText(), schemaName =
                  wSchema.getText();

          // clear
          for ( ColumnInfo tableField : tableFieldColumns ) {
            tableField.setComboValues( new String[]{} );
          }
          if ( !Const.isEmpty( tableName ) ) {
            DatabaseMeta ci = transMeta.findDatabase( connectionName );
            if ( ci != null ) {
              Database db = new Database( loggingObject, ci );
              try {
                db.connect();

                String schemaTable =
                        ci.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( schemaName ), transMeta
                                .environmentSubstitute( tableName ) );
                RowMetaInterface r = db.getTableFields( schemaTable );
                if ( null != r ) {
                  String[] fieldNames = r.getFieldNames();
                  if ( null != fieldNames ) {
                    for ( ColumnInfo tableField : tableFieldColumns ) {
                      tableField.setComboValues( fieldNames );
                    }
                  }
                }
              } catch ( Exception e ) {
                for ( ColumnInfo tableField : tableFieldColumns ) {
                  tableField.setComboValues( new String[]{} );
                }
                // ignore any errors here. drop downs will not be
                // filled, but no problem for the user
              } finally {
                try {
                  //noinspection ConstantConditions
                  if ( db != null ) {
                    db.disconnect();
                  }
                } catch ( Exception ignored ) {
                  // ignore any errors here. Nothing we can do if
                  // connection fails to close properly
                  //noinspection UnusedAssignment
                  db = null;
                }
              }
            }
          }
        }
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  /**
   * Enable and disable fields based on selection changes
   */
  private void setFlags() {
    /////////////////////////////////
    // On Error
    ////////////////////////////////
    if ( wOnError.getSelectionIndex() == SnowflakeBulkLoaderMeta.ON_ERROR_SKIP_FILE ) {
      wlErrorLimit.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Label" ) );
      wlErrorLimit.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorCountLimit.Tooltip" ) );
      wlErrorLimit.setEnabled( true );
      wErrorLimit.setEnabled( true );
    } else if ( wOnError.getSelectionIndex() == SnowflakeBulkLoaderMeta.ON_ERROR_SKIP_FILE_PERCENT ) {
      wlErrorLimit.setText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorPercentLimit.Label" ) );
      wlErrorLimit.setToolTipText( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Dialog.ErrorPercentLimit.Tooltip" ) );
      wlErrorLimit.setEnabled( true );
      wErrorLimit.setEnabled( true );
    } else {
      wlErrorLimit.setEnabled( false );
      wErrorLimit.setEnabled( false );
    }

    ////////////////////////////
    // Location Type
    ////////////////////////////
    if ( wLocationType.getSelectionIndex() == SnowflakeBulkLoaderMeta.LOCATION_TYPE_INTERNAL_STAGE ) {
      wStageName.setEnabled( true );
    } else {
      wStageName.setEnabled( false );
    }

    ////////////////////////////
    // Data Type
    ////////////////////////////

    if ( wDataType.getSelectionIndex() == SnowflakeBulkLoaderMeta.DATA_TYPE_JSON ) {
      gCsvGroup.setVisible( false );
      gJsonGroup.setVisible( true );
      wJsonField.setVisible( true );
      wlJsonField.setVisible( true );
      wSpecifyFields.setVisible( false );
      wlSpecifyFields.setVisible( false );
      wFields.setVisible( false );
      wGet.setVisible( false );
      wDoMapping.setVisible( false );
    } else {
      gCsvGroup.setVisible( true );
      gJsonGroup.setVisible( false );
      wJsonField.setVisible( false );
      wlJsonField.setVisible( false );
      wSpecifyFields.setVisible( true );
      wlSpecifyFields.setVisible( true );
      wFields.setVisible( true );
      wFields.setEnabled( wSpecifyFields.getSelection() );
      wFields.table.setEnabled( wSpecifyFields.getSelection() );
      if ( wSpecifyFields.getSelection() ) {
        wFields.setForeground( display.getSystemColor( SWT.COLOR_GRAY ) );
      } else {
        wFields.setForeground( display.getSystemColor( SWT.COLOR_BLACK ) );
      }
      wGet.setVisible( true );
      wGet.setEnabled( wSpecifyFields.getSelection() );
      wDoMapping.setVisible( true );
      wDoMapping.setEnabled( wSpecifyFields.getSelection() );
    }

  }
}
