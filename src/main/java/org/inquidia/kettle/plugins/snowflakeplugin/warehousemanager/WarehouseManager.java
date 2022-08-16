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

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

import static org.pentaho.di.job.entry.validator.AndValidator.putValidators;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.andValidator;
import static org.pentaho.di.job.entry.validator.JobEntryValidatorUtils.notBlankValidator;


/**
 * Snowflake Warehouse Manager Plugin
 *
 * @author Chris
 * @since 2016-06-08
 */
@SuppressWarnings( { "WeakerAccess", "unused" } )
@JobEntry( id = "SnowflakeWarehouseManager", image = "SWM.svg", name = "JobEntry.Name",
  description = "JobEntry.Description", categoryDescription = "Category.Description",
  i18nPackageName = "org.inquidia.kettle.plugins.snowflakeplugin.warehousemanager",
  documentationUrl = "https://github.com/inquidia/PentahoSnowflakePlugin/wiki/Warehouse-Manager",
  casesUrl = "https://github.com/inquidia/SnowflakePlugin/issues" )
public class WarehouseManager extends JobEntryBase implements Cloneable, JobEntryInterface {
  public static final String MANAGEMENT_ACTION = "managementAction";
  public static final String REPLACE = "replace";
  public static final String FAIL_IF_EXISTS = "failIfExists";
  public static final String WAREHOUSE_NAME = "warehouseName";
  public static final String WAREHOUSE_SIZE = "warehouseSize";
  public static final String WAREHOUSE_TYPE = "warehouseType";
  public static final String MAX_CLUSTER_COUNT = "maxClusterCount";
  public static final String MIN_CLUSTER_COUNT = "minClusterCount";
  public static final String AUTO_SUSPEND = "autoSuspend";
  public static final String AUTO_RESUME = "autoResume";
  public static final String INITIALLY_SUSPENDED = "initiallySuspended";
  public static final String COMMENT = "comment";
  public static final String RESOURCE_MONITOR = "resourceMonitor";
  public static final String CONNECTION = "connection";
  /**
   * The type of management actions this step supports
   */
  private static final String[] MANAGEMENT_ACTIONS = { "create", "drop", "resume", "suspend", "alter" };
  public static final int MANAGEMENT_ACTION_CREATE = 0;
  public static final int MANAGEMENT_ACTION_DROP = 1;
  public static final int MANAGEMENT_ACTION_RESUME = 2;
  public static final int MANAGEMENT_ACTION_SUSPEND = 3;
  public static final int MANAGEMENT_ACTION_ALTER = 4;

  /**
   * The valid warehouse sizes
   */
  private static final String[] WAREHOUSE_SIZES = { "XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE", "XXXLARGE" };
  /**
   * The valid warehouse types
   */
  private static final String[] WAREHOUSE_TYPES = { "Standard", "Enterprise" };
  public static final String FAIL_IF_NOT_EXISTS = "failIfNotExists";
  private static Class<?> PKG = WarehouseManager.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  /**
   * The database to connect to.
   */
  private DatabaseMeta databaseMeta;

  /**
   * The management action to perform.
   */
  private String managementAction;

  /**
   * The name of the warehouse.
   */
  private String warehouseName;

  /**
   * CREATE: If the warehouse exists, should it be replaced
   */
  private boolean replace;

  /**
   * CREATE: Fail if the warehouse exists
   */
  private boolean failIfExists;

  /**
   * DROP: Fail if the warehouse does not exist
   */
  private boolean failIfNotExists;

  /**
   * CREATE: The warehouse size to use
   */
  private String warehouseSize;

  /**
   * CREATE: The warehouse type to use
   */
  private String warehouseType;

  /**
   * CREATE: The maximum cluster size
   */
  private String maxClusterCount;

  /**
   * CREATE: The minimum cluster size
   */
  private String minClusterCount;

  /**
   * CREATE: Should the warehouse automatically suspend
   */
  private String autoSuspend;

  /**
   * CREATE: Should the warehouse automatically resume when it receives a statement
   */
  private boolean autoResume;

  /**
   * CREATE: Should the warehouse start in a suspended state
   */
  private boolean initiallySuspended;

  /**
   * CREATE: The resource monitor to control the warehouse for billing
   */
  private String resourceMonitor;

  /**
   * CREATE: The comment to associate with the statement
   */
  private String comment;

  public WarehouseManager( String name ) {
    super( name, "" );
    setDefault();
    setID( -1L );
  }

  public WarehouseManager() {
    this( "" );
    setDefault();
  }

  public void setDefault() {
    failIfExists = true;
    failIfNotExists = true;
  }

  public Object clone() {
    return super.clone();
  }

  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  public String getManagementAction() {
    return managementAction;
  }

  public void setManagementAction( String managementAction ) {
    this.managementAction = managementAction;
  }

  public int getManagementActionId() {
    if ( managementAction != null ) {
      for ( int i = 0; i < MANAGEMENT_ACTIONS.length; i++ ) {
        if ( managementAction.equals( MANAGEMENT_ACTIONS[i] ) ) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setManagementActionById( int managementActionId ) {
    if ( managementActionId >= 0 && managementActionId <= MANAGEMENT_ACTIONS.length ) {
      managementAction = MANAGEMENT_ACTIONS[managementActionId];
    } else {
      managementAction = null;
    }
  }

  public String getWarehouseName() {
    return warehouseName;
  }

  public void setWarehouseName( String warehouseName ) {
    this.warehouseName = warehouseName;
  }

  public boolean isReplace() {
    return replace;
  }

  public void setReplace( boolean replace ) {
    this.replace = replace;
  }

  public boolean isFailIfExists() {
    return failIfExists;
  }

  public void setFailIfExists( boolean failIfExists ) {
    this.failIfExists = failIfExists;
  }

  public boolean isFailIfNotExists() {
    return failIfNotExists;
  }

  public void setFailIfNotExists( boolean failIfNotExists ) {
    this.failIfNotExists = failIfNotExists;
  }

  public String getWarehouseSize() {
    return warehouseSize;
  }

  public void setWarehouseSize( String warehouseSize ) {
    this.warehouseSize = warehouseSize;
  }

  public int getWarehouseSizeId() {
    if ( warehouseSize != null ) {
      for ( int i = 0; i < WAREHOUSE_SIZES.length; i++ ) {
        if ( warehouseSize.equals( WAREHOUSE_SIZES[i] ) ) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setWarehouseSizeById( int warehouseSizeId ) {
    if ( warehouseSizeId >= 0 && warehouseSizeId < WAREHOUSE_SIZES.length ) {
      warehouseSize = WAREHOUSE_SIZES[warehouseSizeId];
    } else {
      warehouseSize = null;
    }
  }

  public String getWarehouseType() {
    return warehouseType;
  }

  public void setWarehouseType( String warehouseType ) {
    this.warehouseType = warehouseType;
  }

  public int getWarehouseTypeId() {
    if ( warehouseType != null ) {
      for ( int i = 0; i < WAREHOUSE_TYPES.length; i++ ) {
        if ( warehouseType.equals( WAREHOUSE_TYPES[i] ) ) {
          return i;
        }
      }
    }
    return -1;
  }

  public void setWarehouseTypeById( int warehouseTypeId ) {
    if ( warehouseTypeId >= 0 && warehouseTypeId < WAREHOUSE_TYPES.length ) {
      warehouseType = WAREHOUSE_TYPES[warehouseTypeId];
    } else {
      warehouseType = null;
    }
  }

  public String getMaxClusterCount() {
    return maxClusterCount;
  }

  public void setMaxClusterCount( String maxClusterCount ) {
    this.maxClusterCount = maxClusterCount;
  }

  public String getMinClusterCount() {
    return minClusterCount;
  }

  public void setMinClusterCount( String minClusterCount ) {
    this.minClusterCount = minClusterCount;
  }

  public String getAutoSuspend() {
    return autoSuspend;
  }

  public void setAutoSuspend( String autoSuspend ) {
    this.autoSuspend = autoSuspend;
  }

  public boolean isAutoResume() {
    return autoResume;
  }

  public void setAutoResume( boolean autoResume ) {
    this.autoResume = autoResume;
  }

  public boolean isInitiallySuspended() {
    return initiallySuspended;
  }

  public void setInitiallySuspended( boolean initiallySuspended ) {
    this.initiallySuspended = initiallySuspended;
  }

  public String getResourceMonitor() {
    return resourceMonitor;
  }

  public void setResourceMonitor( String resourceMonitor ) {
    this.resourceMonitor = resourceMonitor;
  }

  public String getComment() {
    return comment;
  }

  public void setComment( String comment ) {
    this.comment = comment;
  }

  public String getXML() {
    StringBuffer returnValue = new StringBuffer( 300 );

    returnValue.append( super.getXML() );
    returnValue.append( "      " ).append(
      XMLHandler.addTagValue( CONNECTION, databaseMeta == null ? null : databaseMeta.getName() ) );
    returnValue.append( "      " ).append( XMLHandler.addTagValue( MANAGEMENT_ACTION, getManagementAction() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( REPLACE, isReplace() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( FAIL_IF_EXISTS, isFailIfExists() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( WAREHOUSE_NAME, getWarehouseName() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( WAREHOUSE_SIZE, getWarehouseSize() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( WAREHOUSE_TYPE, getWarehouseType() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( MAX_CLUSTER_COUNT, getMaxClusterCount() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( MIN_CLUSTER_COUNT, getMinClusterCount() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( AUTO_SUSPEND, getAutoSuspend() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( AUTO_RESUME, isAutoResume() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( INITIALLY_SUSPENDED, isInitiallySuspended() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( RESOURCE_MONITOR, getResourceMonitor() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( COMMENT, getComment() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    returnValue.append( "      " ).append( XMLHandler.addTagValue( FAIL_IF_NOT_EXISTS, isFailIfNotExists() ) );

    return returnValue.toString();
  }

  public void loadXML( Node entryNode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers, Repository rep ) throws KettleXMLException {
    try {
      super.loadXML( entryNode, databases, slaveServers );
      String dbname = XMLHandler.getTagValue( entryNode, CONNECTION );
      databaseMeta = DatabaseMeta.findDatabase( databases, dbname );

      setManagementAction( XMLHandler.getTagValue( entryNode, MANAGEMENT_ACTION ) );
      setReplace( "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, REPLACE ) ) );
      setFailIfExists( "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, FAIL_IF_EXISTS ) ) );
      setWarehouseName( XMLHandler.getTagValue( entryNode, WAREHOUSE_NAME ) );
      setWarehouseSize( XMLHandler.getTagValue( entryNode, WAREHOUSE_SIZE ) );
      setWarehouseType( XMLHandler.getTagValue( entryNode, WAREHOUSE_TYPE ) );
      setMaxClusterCount( XMLHandler.getTagValue( entryNode, MAX_CLUSTER_COUNT ) );
      setMinClusterCount( XMLHandler.getTagValue( entryNode, MIN_CLUSTER_COUNT ) );
      setAutoSuspend( XMLHandler.getTagValue( entryNode, AUTO_SUSPEND ) );
      setAutoResume( "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, AUTO_RESUME ) ) );
      setInitiallySuspended( "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, INITIALLY_SUSPENDED ) ) );
      setResourceMonitor( XMLHandler.getTagValue( entryNode, RESOURCE_MONITOR ) );
      setComment( XMLHandler.getTagValue( entryNode, COMMENT ) );
      setFailIfNotExists( "Y".equalsIgnoreCase( XMLHandler.getTagValue( entryNode, FAIL_IF_NOT_EXISTS ) ) );
    } catch ( KettleXMLException dbe ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Error.Exception.UnableLoadXML" ), dbe );
    }
  }

  // Load the jobentry from repository
  public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
                       List<SlaveServer> slaveServers ) throws KettleException {
    try {
      setManagementAction( rep.getJobEntryAttributeString( id_jobentry, MANAGEMENT_ACTION ) );
      setReplace( rep.getJobEntryAttributeBoolean( id_jobentry, REPLACE ) );
      setFailIfExists( rep.getJobEntryAttributeBoolean( id_jobentry, FAIL_IF_EXISTS ) );
      setWarehouseName( rep.getJobEntryAttributeString( id_jobentry, WAREHOUSE_NAME ) );
      setWarehouseSize( rep.getJobEntryAttributeString( id_jobentry, WAREHOUSE_SIZE ) );
      setWarehouseType( rep.getJobEntryAttributeString( id_jobentry, WAREHOUSE_TYPE ) );
      setMaxClusterCount( rep.getJobEntryAttributeString( id_jobentry, MAX_CLUSTER_COUNT ) );
      setMinClusterCount( rep.getJobEntryAttributeString( id_jobentry, MIN_CLUSTER_COUNT ) );
      setAutoSuspend( rep.getJobEntryAttributeString( id_jobentry, AUTO_SUSPEND ) );
      setAutoResume( rep.getJobEntryAttributeBoolean( id_jobentry, AUTO_RESUME ) );
      setInitiallySuspended( rep.getJobEntryAttributeBoolean( id_jobentry, INITIALLY_SUSPENDED ) );
      setResourceMonitor( rep.getJobEntryAttributeString( id_jobentry, RESOURCE_MONITOR ) );
      setComment( rep.getJobEntryAttributeString( id_jobentry, COMMENT ) );
      databaseMeta = rep.loadDatabaseMetaFromJobEntryAttribute( id_jobentry, CONNECTION, "id_database", databases );
      setFailIfNotExists( rep.getJobEntryAttributeBoolean( id_jobentry, FAIL_IF_NOT_EXISTS ) );
    } catch ( KettleException dbe ) {

      throw new KettleException( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Error.Exception.UnableLoadRep" )
        + id_jobentry, dbe );
    }
  }

  // Save the attributes of this job entry
  //
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
    try {
      rep.saveDatabaseMetaJobEntryAttribute( id_job, getObjectId(), CONNECTION, "id_database", databaseMeta );
      rep.saveJobEntryAttribute( id_job, getObjectId(), MANAGEMENT_ACTION, getManagementAction() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), REPLACE, isReplace() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), FAIL_IF_EXISTS, isFailIfExists() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), WAREHOUSE_NAME, getWarehouseName() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), WAREHOUSE_SIZE, getWarehouseSize() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), WAREHOUSE_TYPE, getWarehouseType() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), MAX_CLUSTER_COUNT, getMaxClusterCount() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), MIN_CLUSTER_COUNT, getMinClusterCount() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), AUTO_SUSPEND, getAutoSuspend() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), AUTO_RESUME, isAutoResume() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), INITIALLY_SUSPENDED, isInitiallySuspended() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), RESOURCE_MONITOR, getResourceMonitor() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), COMMENT, getComment() );
      rep.saveJobEntryAttribute( id_job, getObjectId(), FAIL_IF_NOT_EXISTS, isFailIfNotExists() );
    } catch ( KettleDatabaseException dbe ) {
      throw new KettleDatabaseException( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Error.Exception.UnableSaveRep" )
        + getObjectId(), dbe );
    }
  }

  public void clear() {
    super.clear();

    setManagementAction( null );
    setReplace( false );
    setFailIfExists( false );
    setWarehouseName( null );
    setWarehouseSize( null );
    setWarehouseType( null );
    setMaxClusterCount( null );
    setMinClusterCount( null );
    setAutoSuspend( null );
    setAutoResume( false );
    setInitiallySuspended( false );
    setResourceMonitor( null );
    setComment( null );
    setDatabaseMeta( null );
    setFailIfNotExists( true );
  }

  public boolean validate() throws KettleException {
    boolean result = true;
    if ( databaseMeta == null || Const.isEmpty( databaseMeta.getName() ) ) {
      logError( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Validate.DatabaseIsEmpty" ) );
      result = false;
    } else if ( Const.isEmpty( managementAction ) ) {
      logError( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Validate.ManagementAction" ) );
      result = false;
    } else if ( managementAction.equals( MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_CREATE] ) ) {
      if ( !Const.isEmpty( environmentSubstitute( maxClusterCount ) )
        && Const.toInt( environmentSubstitute( maxClusterCount ), -1 ) <= 0 ) {

        logError( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Validate.MaxClusterCount",
          environmentSubstitute( maxClusterCount ) ) );
        return false;
      }

      if ( !Const.isEmpty( environmentSubstitute( minClusterCount ) )
        && Const.toInt( environmentSubstitute( minClusterCount ), -1 ) < 0 ) {

        logError( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Validate.MinClusterCount",
          environmentSubstitute( minClusterCount ) ) );
        return false;
      }

      if ( !Const.isEmpty( environmentSubstitute( autoSuspend ) )
        && Const.toInt( environmentSubstitute( autoSuspend ), -1 ) < 0 ) {
        logError( BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Validate.AutoSuspend",
          environmentSubstitute( autoSuspend ) ) );
        return false;
      }
    }
    return result;
  }

  public Result execute( Result previousResult, int nr ) throws KettleException {

    Result result = previousResult;
    result.setResult( validate() );
    if ( !result.getResult() ) {
      return result;
    }

    Database db = null;
    try {
      db = new Database( this, databaseMeta );
      String SQL = null;
      String successMessage = null;

      if ( managementAction.equals( MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_CREATE] ) ) {
        SQL = getCreateSQL();
        successMessage = BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Log.Create.Success" );
      } else if ( managementAction.equals( MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_DROP] ) ) {
        SQL = getDropSQL();
        successMessage = BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Log.Drop.Success" );
      } else if ( managementAction.equals( MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_RESUME] ) ) {
        SQL = getResumeSQL();
        successMessage = BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Log.Resume.Success" );
      } else if ( managementAction.equals( MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_SUSPEND] ) ) {
        SQL = getSuspendSQL();
        successMessage = BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Log.Suspend.Success" );
      } else if ( managementAction.equals( MANAGEMENT_ACTIONS[MANAGEMENT_ACTION_ALTER] ) ) {
        SQL = getAlterSQL();
        successMessage = BaseMessages.getString( PKG, "SnowflakeWarehouseManager.Log.Alter.Success" );
      }

      if ( SQL == null ) {
        throw new KettleException( "Unable to generate action, could not find action type" );
      }

      db.connect();
      logDebug( "Executing SQL " + SQL );
      db.execStatements( SQL );
      logBasic( successMessage );

    } catch( Exception ex ) {
      logError( "Error managing warehouse", ex );
      result.setResult( false );
    } finally {
      try {
        if ( db != null ) {
          db.disconnect();
        }
      } catch ( Exception ex ) {
        logError( "Unable to disconnect from database", ex );
      }
    }

    return result;

  }

  private String getDropSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append( "DROP WAREHOUSE " );
    if ( !failIfNotExists ) {
      sql.append( "IF EXISTS " );
    }
    sql.append( environmentSubstitute( warehouseName ) ).append( ";\ncommit;" );
    return sql.toString();
  }

  private String getResumeSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append( "ALTER WAREHOUSE " );
    if ( !failIfNotExists ) {
      sql.append( "IF EXISTS " );
    }
    sql.append( environmentSubstitute( warehouseName ) ).append( " RESUME;\ncommit;" );
    return sql.toString();
  }

  private String getSuspendSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append( "ALTER WAREHOUSE " );
    if ( !failIfNotExists ) {
      sql.append( "IF EXISTS " );
    }
    sql.append( environmentSubstitute( warehouseName ) ).append( " SUSPEND;\ncommit;" );
    return sql.toString();
  }

  private String getCreateSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append( "CREATE " );
    if ( replace ) {
      sql.append( "OR REPLACE " );
    }
    sql.append( "WAREHOUSE " );
    if ( !failIfExists && !replace ) {
      sql.append( "IF NOT EXISTS " );
    }
    sql.append( warehouseName ).append( " WITH " );

    if ( !Const.isEmpty( environmentSubstitute( warehouseSize ) ) ) {
      sql.append( "WAREHOUSE_SIZE = '" ).append( environmentSubstitute( warehouseSize ) ).append( "' " );
    }

    if ( !Const.isEmpty( environmentSubstitute( warehouseType ) ) ) {
      sql.append( "WAREHOUSE_TYPE = " ).append( environmentSubstitute( warehouseType ) ).append( " " );
    }

    if ( !Const.isEmpty( environmentSubstitute( maxClusterCount ) ) ) {
      sql.append( "MAX_CLUSTER_COUNT = " ).append( environmentSubstitute( maxClusterCount ) ).append( " " );
    }

    if ( !Const.isEmpty( environmentSubstitute( minClusterCount ) ) ) {
      sql.append( "MIN_CLUSTER_COUNT = " ).append( environmentSubstitute( minClusterCount ) ).append( " " );
    }

    if ( !Const.isEmpty( environmentSubstitute( autoSuspend ) ) ) {
      sql.append( "AUTO_SUSPEND = " ).append( Const.toInt( environmentSubstitute( autoSuspend ), 0 ) * 60 ).append( " " );
    }

    sql.append( "AUTO_RESUME = " ).append( autoResume ).append( " " );
    sql.append( "INITIALLY_SUSPENDED = " ).append( initiallySuspended ).append( " " );

    if ( !Const.isEmpty( environmentSubstitute( resourceMonitor ) ) ) {
      sql.append( "RESOURCE_MONITOR = '" ).append( environmentSubstitute( resourceMonitor ) ).append( "' " );
    }

    if ( !Const.isEmpty( environmentSubstitute( comment ) ) ) {
      sql.append( "COMMENT = \"" ).append( comment.replaceAll( "\"", "\"\"" ) ).append( "\" " );
    }

    sql.append( ";\ncommit;" );
    return sql.toString();
  }

  private String getAlterSQL() {
    StringBuilder sql = new StringBuilder();
    sql.append( "ALTER WAREHOUSE " );
    if ( !failIfNotExists ) {
      sql.append( "IF EXISTS " );
    }
    sql.append( warehouseName ).append( " SET " );

    if ( !Const.isEmpty( environmentSubstitute( warehouseSize ) ) ) {
      sql.append( "WAREHOUSE_SIZE = '" ).append( environmentSubstitute( warehouseSize ) ).append( "' " );
    }

    if ( !Const.isEmpty( environmentSubstitute( warehouseType ) ) ) {
      sql.append( "WAREHOUSE_TYPE = " ).append( environmentSubstitute( warehouseType ) ).append( " " );
    }

    if ( !Const.isEmpty( environmentSubstitute( maxClusterCount ) ) ) {
      sql.append( "MAX_CLUSTER_COUNT = " ).append( environmentSubstitute( maxClusterCount ) ).append( " " );
    }

    if ( !Const.isEmpty( environmentSubstitute( minClusterCount ) ) ) {
      sql.append( "MIN_CLUSTER_COUNT = " ).append( environmentSubstitute( minClusterCount ) ).append( " " );
    }

    if ( !Const.isEmpty( environmentSubstitute( autoSuspend ) ) ) {
      sql.append( "AUTO_SUSPEND = " ).append( Const.toInt( environmentSubstitute( autoSuspend ), 0 ) * 60 ).append( " " );
    }

    sql.append( "AUTO_RESUME = " ).append( autoResume ).append( " " );

    if ( !Const.isEmpty( environmentSubstitute( resourceMonitor ) ) ) {
      sql.append( "RESOURCE_MONITOR = '" ).append( environmentSubstitute( resourceMonitor ) ).append( "' " );
    }

    if ( !Const.isEmpty( environmentSubstitute( comment ) ) ) {
      sql.append( "COMMENT = \"" ).append( comment.replaceAll( "\"", "\"\"" ) ).append( "\" " );
    }

    sql.append( ";\ncommit;" );
    return sql.toString();
  }


  public boolean evaluates() {
    return true;
  }

  public boolean isUnconditional() {
    return true;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, JobMeta jobMeta ) {
    andValidator().validate( this, CONNECTION, remarks, putValidators( notBlankValidator() ) );
    andValidator().validate( this, WAREHOUSE_NAME, remarks, putValidators( notBlankValidator() ) );
    andValidator().validate( this, MANAGEMENT_ACTION, remarks, putValidators( notBlankValidator() ) );
  }


}
