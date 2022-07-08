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

package main.java.org.inquidia.kettle.plugins.snowflakeplugin.bulkloader;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


/**
 * Defines the metadata for the Snowflake Bulk Loader step.  Provides functions to store
 * and read this metadata in ktr files and on a repository.
 */
@SuppressWarnings( "WeakerAccess" )
@Step( id = "SnowflakeBulkLoader", image = "SBL.svg", name = "Step.Name", description = "Step.Description",
  categoryDescription = "Category.Description",
  i18nPackageName = "org.inquidia.kettle.plugins.snowflakeplugin.bulkloader",
  documentationUrl = "https://github.com/inquidia/SnowflakePlugin/wiki/Bulk-Loader",
  casesUrl = "https://github.com/inquidia/SnowflakePlugin/issues",
  isSeparateClassLoaderNeeded = true )
@InjectionSupported( localizationPrefix = "SnowflakeBulkLoader.Injection.", groups = { "OUTPUT_FIELDS" } )
public class SnowflakeBulkLoaderMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = SnowflakeBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  protected static final String DEBUG_MODE_VAR = "${SNOWFLAKE_DEBUG_MODE}";
  /*
   * Static constants for the identifiers used when saving the step to XML or the repository
   */
  private static final String CONNECTION = "connection";
  private static final String TARGET_SCHEMA = "target_schema";
  private static final String TARGET_TABLE = "target_table";
  private static final String LOCATION_TYPE = "location_type";
  private static final String STAGE_NAME = "stage_name";
  private static final String WORK_DIRECTORY = "work_directory";
  private static final String ON_ERROR = "on_error";
  private static final String ERROR_LIMIT = "error_limit";
  private static final String SPLIT_SIZE = "split_size";
  private static final String REMOVE_FILES = "remove_files";
  private static final String DATA_TYPE = "data_type";
  private static final String TRIM_WHITESPACE = "trim_whitespace";
  private static final String NULL_IF = "null_if";
  private static final String ERROR_COLUMN_MISMATCH = "error_column_mismatch";
  private static final String STRIP_NULL = "strip_null";
  private static final String IGNORE_UTF_8 = "ignore_utf8";
  private static final String ALLOW_DUPLICATE_ELEMENT = "allow_duplicate_element";
  private static final String ENABLE_OCTAL = "enable_octal";
  private static final String SPECIFY_FIELDS = "specify_fields";
  private static final String JSON_FIELD = "json_field";
  private static final String FIELDS = "fields";
  private static final String FIELD = "field";
  private static final String STREAM_FIELD = "stream_field";
  private static final String TABLE_FIELD = "table_field";

  /*
   * Static constants used for the bulk loader when creating temp files.
   */
  public static final String CSV_DELIMITER = ",";
  public static final String CSV_RECORD_DELIMITER = "\n";
  public static final String CSV_ESCAPE_CHAR = "\\";
  public static final String ENCLOSURE = "\"";
  public static final String DATE_FORMAT_STRING = "yyyy-MM-dd";
  public static final String TIMESTAMP_FORMAT_STRING = "YYYY-MM-DD HH24:MI:SS.FF3";

  /**
   * The valid location type codes {@value}
   */
  public static final String[] LOCATION_TYPE_CODES = { "user", "table", "internal_stage" };
  public static final int LOCATION_TYPE_USER = 0;
  public static final int LOCATION_TYPE_TABLE = 1;
  public static final int LOCATION_TYPE_INTERNAL_STAGE = 2;

  /**
   * The valid on error codes {@value}
   */
  public static final String[] ON_ERROR_CODES = { "continue", "skip_file", "skip_file_percent", "abort" };
  public static final int ON_ERROR_CONTINUE = 0;
  public static final int ON_ERROR_SKIP_FILE = 1;
  public static final int ON_ERROR_SKIP_FILE_PERCENT = 2;
  public static final int ON_ERROR_ABORT = 3;

  /**
   * The valid data type codes {@value}
   */
  public static final String[] DATA_TYPE_CODES = { "csv", "json" };
  public static final int DATA_TYPE_CSV = 0;
  public static final int DATA_TYPE_JSON = 1;

  /**
   * The date appended to the filenames
   */
  private String fileDate;

  /**
   * The database connection to use
   */
  private DatabaseMeta databaseMeta;

  /**
   * The schema to use
   */
  @Injection( name = "TARGET_SCHEMA" )
  private String targetSchema;

  /**
   * The table to load
   */
  @Injection( name = "TARGET_TABLE" )
  private String targetTable;

  /**
   * The location type (user, table, internal_stage)
   */
  @Injection( name = "LOCATION_TYPE" )
  private String locationType;

  /**
   * If location type = Internal stage, the stage name to use
   */
  @Injection( name = "STAGE_NAME" )
  private String stageName;

  /**
   * The work directory to use when writing temp files
   */
  @Injection( name = "WORK_DIRECTORY" )
  private String workDirectory;

  /**
   * What to do when an error is encountered (continue, skip_file, skip_file_percent, abort)
   */
  @Injection( name = "ON_ERROR" )
  private String onError;

  /**
   * When On Error = Skip File, the number of error rows before skipping the file, if 0 skip immediately.
   * When On Error = Skip File Percent, the percentage of the file to error before skipping the file.
   */
  @Injection( name = "ERROR_LIMIT" )
  private String errorLimit;

  /**
   * The size to split the data at to enable faster bulk loading
   */
  @Injection( name = "SPLIT_SIZE" )
  private String splitSize;

  /**
   * Should the files loaded to the staging location be removed
   */
  @Injection( name = "REMOVE_FILES" )
  private boolean removeFiles;

  /**
   * The target step for bulk loader output
   */
  @Injection( name = "OUTPUT_TARGET_STEP" )
  private String outputTargetStep;

  /**
   * The data type of the data (csv, json)
   */
  @Injection( name = "DATA_TYPE" )
  private String dataType;

  /**
   * CSV: Trim whitespace
   */
  @Injection( name = "TRIM_WHITESPACE" )
  private boolean trimWhitespace;

  /**
   * CSV: Convert column value to null if
   */
  @Injection( name = "NULL_IF" )
  private String nullIf;

  /**
   * CSV: Should the load fail if the column count in the row does not match the column count in the table
   */
  @Injection( name = "ERROR_COLUMN_MISMATCH" )
  private boolean errorColumnMismatch;

  /**
   * JSON: Strip nulls from JSON
   */
  @Injection( name = "STRIP_NULL" )
  private boolean stripNull;

  /**
   * JSON: Ignore UTF8 Errors
   */
  @Injection( name = "IGNORE_UTF8" )
  private boolean ignoreUtf8;

  /**
   * JSON: Allow duplicate elements
   */
  @Injection( name = "ALLOW_DUPLICATE_ELEMENTS" )
  private boolean allowDuplicateElements;

  /**
   * JSON: Enable Octal number parsing
   */
  @Injection( name = "ENABLE_OCTAL" )
  private boolean enableOctal;

  /**
   * CSV: Specify field to table mapping
   */
  @Injection( name = "SPECIFY_FIELDS" )
  private boolean specifyFields;

  /**
   * JSON: JSON field name
   */
  @Injection( name = "JSON_FIELD" )
  private String jsonField;

  /**
   * CSV: The field mapping from the Stream to the database
   */
  @InjectionDeep
  private SnowflakeBulkLoaderField[] snowflakeBulkLoaderFields;

  /**
   * Default initializer
   */
  public SnowflakeBulkLoaderMeta() {
    super(); // allocate BaseStepMeta
    allocate( 0 );
  }

  /**
   * @return The metadata of the database connection to use when bulk loading
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * Set the database connection to use
   *
   * @param databaseMeta The metadata for the database connection
   */
  public void setDatabaseMeta( DatabaseMeta databaseMeta ) {
    this.databaseMeta = databaseMeta;
  }

  /**
   * @return The schema to load
   */
  public String getTargetSchema() {
    return targetSchema;
  }

  /**
   * Set the schema to load
   *
   * @param targetSchema The schema name
   */
  public void setTargetSchema( String targetSchema ) {
    this.targetSchema = targetSchema;
  }

  /**
   * @return The table name to load
   */
  public String getTargetTable() {
    return targetTable;
  }

  /**
   * Set the table name to load
   *
   * @param targetTable The table name
   */
  public void setTargetTable( String targetTable ) {
    this.targetTable = targetTable;
  }

  /**
   * @return The location type code for the files to load
   */
  public String getLocationType() {
    return locationType;
  }

  /**
   * Set the location type code to use
   *
   * @param locationType The location type code from @LOCATION_TYPE_CODES
   * @throws KettleException Invalid location type
   */
  @SuppressWarnings( "unused" )
  public void setLocationType( String locationType ) throws KettleException {
    for ( String LOCATION_TYPE_CODE : LOCATION_TYPE_CODES ) {
      if ( LOCATION_TYPE_CODE.equals( locationType ) ) {
        this.locationType = locationType;
        return;
      }
    }

    // No matching location type, the location type is invalid
    throw new KettleException( "Invalid location type " + locationType );
  }

  /**
   * @return The ID of the location type
   */
  public int getLocationTypeId() {
    for ( int i = 0; i < LOCATION_TYPE_CODES.length; i++ ) {
      if ( LOCATION_TYPE_CODES[i].equals( locationType ) ) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Takes an ID for the location type and sets the location type
   *
   * @param locationTypeId The location type id
   */
  public void setLocationTypeById( int locationTypeId ) {
    locationType = LOCATION_TYPE_CODES[locationTypeId];
  }

  /**
   * Ignored unless the location_type is internal_stage
   *
   * @return The name of the Snowflake stage
   */
  @SuppressWarnings( "unused" )
  public String getStageName() {
    return stageName;
  }

  /**
   * Ignored unless the location_type is internal_stage, sets the name of the Snowflake stage
   *
   * @param stageName The name of the Snowflake stage
   */
  @SuppressWarnings( "unused" )
  public void setStageName( String stageName ) {
    this.stageName = stageName;
  }

  /**
   * @return The local work directory to store temporary files
   */
  public String getWorkDirectory() {
    return workDirectory;
  }

  /**
   * Set the local word directory to store temporary files.  The directory must exist.
   *
   * @param workDirectory The path to the work directory
   */
  public void setWorkDirectory( String workDirectory ) {
    this.workDirectory = workDirectory;
  }

  /**
   * @return The code from @ON_ERROR_CODES to use when an error occurs during the load
   */
  public String getOnError() {
    return onError;
  }

  /**
   * Set the behavior for what to do when an error occurs during the load
   *
   * @param onError The error code from @ON_ERROR_CODES
   * @throws KettleException
   */
  @SuppressWarnings( "unused" )
  public void setOnError( String onError ) throws KettleException {
    for ( String ON_ERROR_CODE : ON_ERROR_CODES ) {
      if ( ON_ERROR_CODE.equals( onError ) ) {
        this.onError = onError;
        return;
      }
    }

    // No matching on error codes, we have a problem
    throw new KettleException( "Invalid on error code " + onError );
  }

  /**
   * Gets the ID for the onError method being used
   *
   * @return The ID for the onError method being used
   */
  public int getOnErrorId() {
    for ( int i = 0; i < ON_ERROR_CODES.length; i++ ) {
      if ( ON_ERROR_CODES[i].equals( onError ) ) {
        return i;
      }
    }
    return -1;
  }

  /**
   * @param onErrorId The ID of the error method
   */
  public void setOnErrorById( int onErrorId ) {
    onError = ON_ERROR_CODES[onErrorId];
  }

  /**
   * Ignored if onError is not skip_file or skip_file_percent
   *
   * @return The limit at which to fail
   */
  public String getErrorLimit() {
    return errorLimit;
  }

  /**
   * Ignored if onError is not skip_file or skip_file_percent, the limit at which Snowflake should skip loading the file
   *
   * @param errorLimit The limit at which Snowflake should skip loading the file.  0 = no limit
   */
  public void setErrorLimit( String errorLimit ) {
    this.errorLimit = errorLimit;
  }

  /**
   * @return The number of rows at which the files should be split
   */
  public String getSplitSize() {
    return splitSize;
  }

  /**
   * Set the number of rows at which to split files
   *
   * @param splitSize The size to split at in number of rows
   */
  public void setSplitSize( String splitSize ) {
    this.splitSize = splitSize;
  }

  /**
   * @return Should the files be removed from the Snowflake internal storage after they are loaded
   */
  public boolean isRemoveFiles() {
    return removeFiles;
  }

  /**
   * Set if the files should be removed from the Snowflake internal storage after they are loaded
   *
   * @param removeFiles true/false
   */
  public void setRemoveFiles( boolean removeFiles ) {
    this.removeFiles = removeFiles;
  }

  /**
   * @return The step to direct the output data to.
   */
  public String getOutputTargetStep() {
    return outputTargetStep;
  }

  /**
   * Set the step to direct bulk loader output to.
   * @param outputTargetStep The step name
   */
  public void setOutputTargetStep( String outputTargetStep ) {
    this.outputTargetStep = outputTargetStep;
  }

  /**
   * @return The data type code being loaded from @DATA_TYPE_CODES
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * Set the data type
   *
   * @param dataType The data type code from @DATA_TYPE_CODES
   * @throws KettleException Invalid value
   */
  @SuppressWarnings( "unused" )
  public void setDataType( String dataType ) throws KettleException {
    for ( String DATA_TYPE_CODE : DATA_TYPE_CODES ) {
      if ( DATA_TYPE_CODE.equals( dataType ) ) {
        this.dataType = dataType;
        return;
      }
    }

    //No matching data type
    throw new KettleException( "Invalid data type " + dataType );
  }

  /**
   * Gets the data type ID, which is equivalent to the location of the data type code within the
   * (DATA_TYPE_CODES) array
   * @return The ID of the data type
   */
  public int getDataTypeId() {
    for ( int i = 0; i < DATA_TYPE_CODES.length; i++ ) {
      if ( DATA_TYPE_CODES[i].equals( dataType ) ) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Takes the ID of the data type and sets the data type code to the equivalent location within the
   * DATA_TYPE_CODES array
   * @param dataTypeId The ID of the data type
   */
  public void setDataTypeById( int dataTypeId ) {
    dataType = DATA_TYPE_CODES[dataTypeId];
  }

  /**
   * CSV:
   *
   * @return Should whitespace in the fields be trimmed
   */
  public boolean isTrimWhitespace() {
    return trimWhitespace;
  }

  /**
   * CSV: Set if the whitespace in the files should be trimmmed
   *
   * @param trimWhitespace true/false
   */
  public void setTrimWhitespace( boolean trimWhitespace ) {
    this.trimWhitespace = trimWhitespace;
  }

  /**
   * CSV:
   *
   * @return Comma delimited list of strings to convert to Null
   */
  public String getNullIf() {
    return nullIf;
  }

  /**
   * CSV: Set the string constants to convert to Null
   *
   * @param nullIf Comma delimited list of constants
   */
  public void setNullIf( String nullIf ) {
    this.nullIf = nullIf;
  }

  /**
   * CSV:
   *
   * @return Should the load error if the number of columns in the table and in the CSV do not match
   */
  public boolean isErrorColumnMismatch() {
    return errorColumnMismatch;
  }

  /**
   * CSV: Set if the load should error if the number of columns in the table and in the CSV do not match
   *
   * @param errorColumnMismatch true/false
   */
  public void setErrorColumnMismatch( boolean errorColumnMismatch ) {
    this.errorColumnMismatch = errorColumnMismatch;
  }

  /**
   * JSON:
   *
   * @return Should null values be stripped out of the JSON
   */
  public boolean isStripNull() {
    return stripNull;
  }

  /**
   * JSON: Set if null values should be stripped out of the JSON
   *
   * @param stripNull true/false
   */
  public void setStripNull( boolean stripNull ) {
    this.stripNull = stripNull;
  }

  /**
   * JSON:
   *
   * @return Should UTF8 errors be ignored
   */
  public boolean isIgnoreUtf8() {
    return ignoreUtf8;
  }

  /**
   * JSON: Set if UTF8 errors should be ignored
   *
   * @param ignoreUtf8 true/false
   */
  public void setIgnoreUtf8( boolean ignoreUtf8 ) {
    this.ignoreUtf8 = ignoreUtf8;
  }

  /**
   * JSON:
   *
   * @return Should duplicate element names in the JSON be allowed. If true the last value for the name is used.
   */
  public boolean isAllowDuplicateElements() {
    return allowDuplicateElements;
  }

  /**
   * JSON: Set if duplicate element names in the JSON be allowed.  If true the last value for the name is used.
   *
   * @param allowDuplicateElements true/false
   */
  public void setAllowDuplicateElements( boolean allowDuplicateElements ) {
    this.allowDuplicateElements = allowDuplicateElements;
  }

  /**
   * JSON: Should processing of octal based numbers be enabled?
   *
   * @return Is octal number parsing enabled?
   */
  public boolean isEnableOctal() {
    return enableOctal;
  }

  /**
   * JSON: Set if processing of octal based numbers should be enabled
   *
   * @param enableOctal true/false
   */
  public void setEnableOctal( boolean enableOctal ) {
    this.enableOctal = enableOctal;
  }

  /**
   * CSV: Is the mapping of stream fields to table fields being specified?
   *
   * @return Are fields being specified?
   */
  public boolean isSpecifyFields() {
    return specifyFields;
  }

  /**
   * CSV: Set if the mapping of stream fields to table fields is being specified
   *
   * @param specifyFields true/false
   */
  public void setSpecifyFields( boolean specifyFields ) {
    this.specifyFields = specifyFields;
  }

  /**
   * JSON: The stream field containing the JSON string.
   *
   * @return The stream field containing the JSON
   */
  public String getJsonField() {
    return jsonField;
  }

  /**
   * JSON: Set the input stream field containing the JSON string.
   *
   * @param jsonField The stream field containing the JSON
   */
  public void setJsonField( String jsonField ) {
    this.jsonField = jsonField;
  }

  /**
   * CSV: Get the array containing the Stream to Table field mapping
   *
   * @return The array containing the stream to table field mapping
   */
  public SnowflakeBulkLoaderField[] getSnowflakeBulkLoaderFields() {
    return snowflakeBulkLoaderFields;
  }

  /**
   * CSV: Set the array containing the Stream to Table field mapping
   *
   * @param snowflakeBulkLoaderFields The array containing the stream to table field mapping
   */
  @SuppressWarnings( "unused" )
  public void setSnowflakeBulkLoaderFields( SnowflakeBulkLoaderField[] snowflakeBulkLoaderFields ) {
    this.snowflakeBulkLoaderFields = snowflakeBulkLoaderFields;
  }

  /**
   * Get the file date that is appended in the file names
   * @return The file date that is appended in the file names
   */
  public String getFileDate() {
    return fileDate;
  }

  /**
   * Loads the step metadata from the XML ktr file
   *
   * @param stepNode  The node in the XML
   * @param databases The list of databases
   * @param metaStore The metastore
   * @throws KettleXMLException
   */
  public void loadXML( Node stepNode, List<DatabaseMeta> databases, IMetaStore metaStore )
    throws KettleXMLException {
    readData( stepNode, databases );
  }

  /**
   * Allocates the number of fields to use in the field mapping
   *
   * @param numberFields The number of fields to use in the field mapping
   */
  public void allocate( int numberFields ) {
    snowflakeBulkLoaderFields = new SnowflakeBulkLoaderField[numberFields];
  }

  /**
   * Clones the step so that it can be copied and used in clusters
   *
   * @return A copy of the step
   */
  public Object clone() {
    SnowflakeBulkLoaderMeta retval = (SnowflakeBulkLoaderMeta) super.clone();
    int nrfields = snowflakeBulkLoaderFields.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.snowflakeBulkLoaderFields[i] = (SnowflakeBulkLoaderField) snowflakeBulkLoaderFields[i].clone();
    }

    return retval;
  }

  /**
   * Reads the XML data to get the step metadata
   *
   * @param stepNode The XML node for the step
   * @throws KettleXMLException
   */
  private void readData( Node stepNode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
    try {
      databaseMeta = DatabaseMeta.findDatabase( databases, XMLHandler.getTagValue( stepNode, CONNECTION ) );

      targetSchema = XMLHandler.getTagValue( stepNode, TARGET_SCHEMA );
      targetTable = XMLHandler.getTagValue( stepNode, TARGET_TABLE );
      locationType = XMLHandler.getTagValue( stepNode, LOCATION_TYPE );
      stageName = XMLHandler.getTagValue( stepNode, STAGE_NAME );
      workDirectory = XMLHandler.getTagValue( stepNode, WORK_DIRECTORY );
      onError = XMLHandler.getTagValue( stepNode, ON_ERROR );
      errorLimit = XMLHandler.getTagValue( stepNode, ERROR_LIMIT );
      splitSize = XMLHandler.getTagValue( stepNode, SPLIT_SIZE );
      removeFiles = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, REMOVE_FILES ) );

      dataType = XMLHandler.getTagValue( stepNode, DATA_TYPE );
      trimWhitespace = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, TRIM_WHITESPACE ) );
      nullIf = XMLHandler.getTagValue( stepNode, NULL_IF );
      errorColumnMismatch = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, ERROR_COLUMN_MISMATCH ) );
      stripNull = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, STRIP_NULL ) );
      ignoreUtf8 = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, IGNORE_UTF_8 ) );
      allowDuplicateElements = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, ALLOW_DUPLICATE_ELEMENT ) );
      enableOctal = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, ENABLE_OCTAL ) );

      specifyFields = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepNode, SPECIFY_FIELDS ) );
      jsonField = XMLHandler.getTagValue( stepNode, JSON_FIELD );

      Node fields = XMLHandler.getSubNode( stepNode, FIELDS );
      int nrfields = XMLHandler.countNodes( fields, FIELD );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, FIELD, i );

        snowflakeBulkLoaderFields[i] = new SnowflakeBulkLoaderField();
        snowflakeBulkLoaderFields[i].setStreamField( XMLHandler.getTagValue( fnode, STREAM_FIELD ) );
        snowflakeBulkLoaderFields[i].setTableField( XMLHandler.getTagValue( fnode, TABLE_FIELD ) );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  /**
   * Sets the default values for all metadata attributes.
   */
  public void setDefault() {
    locationType = LOCATION_TYPE_CODES[LOCATION_TYPE_USER];
    workDirectory = "${java.io.tmpdir}";
    onError = ON_ERROR_CODES[ON_ERROR_ABORT];
    removeFiles = true;

    dataType = DATA_TYPE_CODES[DATA_TYPE_CSV];
    trimWhitespace = false;
    errorColumnMismatch = true;
    stripNull = false;
    ignoreUtf8 = false;
    allowDuplicateElements = false;
    enableOctal = false;
    splitSize = "20000";

    specifyFields = false;
  }

  /**
   * Builds a filename for a temporary file  The filename is in tableName_date_time_stepnr_partnr_splitnr.gz format
   *
   * @param space       The variables currently set
   * @param stepNumber  The step number.  Used when multiple copies of the step are started.
   * @param partNumber  The partition number.  Used when the transformation is executed clustered, the number of the
   *                    partition.
   * @param splitNumber The split number.  Used when the file is split into multiple chunks.
   * @return The filename to use
   */
  public String buildFilename( VariableSpace space, int stepNumber, String partNumber,
                               int splitNumber ) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realWorkDirectory = space.environmentSubstitute( workDirectory );

    //Files are always gzipped
    String extension = ".gz";

    StringBuilder returnValue = new StringBuilder( realWorkDirectory );
    if ( !realWorkDirectory.endsWith( "/" ) && !realWorkDirectory.endsWith( "\\" ) ) {
      returnValue.append( Const.FILE_SEPARATOR );
    }

    returnValue.append( targetTable ).append( "_" );

    if ( fileDate == null ) {

      Date now = new Date();

      daf.applyPattern( "yyyyMMdd_HHmmss" );
      fileDate = daf.format( now );
    }
    returnValue.append( fileDate ).append( "_" );

    returnValue.append( stepNumber ).append( "_" );
    returnValue.append( partNumber ).append( "_" );
    returnValue.append( splitNumber );
    returnValue.append( extension );

    return returnValue.toString();
  }

  /**
   * Creates the XML to store in the ktr file
   *
   * @return A string containing the XML to store the step in a KTR file
   */
  public String getXML() {
    StringBuilder returnValue = new StringBuilder( 800 );

    returnValue.append( "    " ).append( XMLHandler.addTagValue( CONNECTION, databaseMeta == null ? "" : databaseMeta
      .getName() ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( TARGET_SCHEMA, targetSchema ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( TARGET_TABLE, targetTable ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( LOCATION_TYPE, locationType ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( STAGE_NAME, stageName ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( WORK_DIRECTORY, workDirectory ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( ON_ERROR, onError ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( ERROR_LIMIT, errorLimit ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( SPLIT_SIZE, splitSize ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( REMOVE_FILES, removeFiles ) );

    returnValue.append( "    " ).append( XMLHandler.addTagValue( DATA_TYPE, dataType ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( TRIM_WHITESPACE, trimWhitespace ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( NULL_IF, nullIf ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( ERROR_COLUMN_MISMATCH, errorColumnMismatch ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( STRIP_NULL, stripNull ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( IGNORE_UTF_8, ignoreUtf8 ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( ALLOW_DUPLICATE_ELEMENT, allowDuplicateElements ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( ENABLE_OCTAL, enableOctal ) );

    returnValue.append( "    " ).append( XMLHandler.addTagValue( JSON_FIELD, jsonField ) );
    returnValue.append( "    " ).append( XMLHandler.addTagValue( SPECIFY_FIELDS, specifyFields ) );

    returnValue.append( "    <fields>" ).append( Const.CR );
    for ( SnowflakeBulkLoaderField field : snowflakeBulkLoaderFields ) {
      if ( field.getStreamField() != null && field.getStreamField().length() != 0 ) {
        returnValue.append( "      <field>" ).append( Const.CR );
        returnValue.append( "        " ).append( XMLHandler.addTagValue( STREAM_FIELD, field.getStreamField() ) );
        returnValue.append( "        " ).append( XMLHandler.addTagValue( TABLE_FIELD, field.getTableField() ) );
        returnValue.append( "      </field>" ).append( Const.CR );
      }
    }
    returnValue.append( "    </fields>" ).append( Const.CR );

    return returnValue.toString();
  }

  /**
   * Read the metadata for the step from the repository
   *
   * @param rep       The repository
   * @param metaStore The metastore
   * @param id_step   The ID of the step
   * @param databases The list of the databases
   * @throws KettleException
   */
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      databaseMeta = rep.loadDatabaseMetaFromStepAttribute( id_step, CONNECTION, databases );
      targetSchema = rep.getStepAttributeString( id_step, TARGET_SCHEMA );
      targetTable = rep.getStepAttributeString( id_step, TARGET_TABLE );
      locationType = rep.getJobEntryAttributeString( id_step, LOCATION_TYPE );
      stageName = rep.getStepAttributeString( id_step, STAGE_NAME );
      workDirectory = rep.getStepAttributeString( id_step, WORK_DIRECTORY );
      onError = rep.getStepAttributeString( id_step, ON_ERROR );
      errorLimit = rep.getStepAttributeString( id_step, ERROR_LIMIT );
      splitSize = rep.getStepAttributeString( id_step, SPLIT_SIZE );
      removeFiles = rep.getStepAttributeBoolean( id_step, REMOVE_FILES );

      dataType = rep.getStepAttributeString( id_step, DATA_TYPE );
      trimWhitespace = rep.getStepAttributeBoolean( id_step, TRIM_WHITESPACE );
      nullIf = rep.getStepAttributeString( id_step, NULL_IF );
      errorColumnMismatch = rep.getStepAttributeBoolean( id_step, ERROR_COLUMN_MISMATCH );

      stripNull = rep.getStepAttributeBoolean( id_step, STRIP_NULL );
      ignoreUtf8 = rep.getStepAttributeBoolean( id_step, IGNORE_UTF_8 );
      allowDuplicateElements = rep.getStepAttributeBoolean( id_step, ALLOW_DUPLICATE_ELEMENT );
      enableOctal = rep.getStepAttributeBoolean( id_step, ENABLE_OCTAL );

      specifyFields = rep.getStepAttributeBoolean( id_step, SPECIFY_FIELDS );
      jsonField = rep.getStepAttributeString( id_step, JSON_FIELD );

      int nrfields = rep.countNrStepAttributes( id_step, STREAM_FIELD );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        snowflakeBulkLoaderFields[i] = new SnowflakeBulkLoaderField();

        snowflakeBulkLoaderFields[i].setStreamField( rep.getStepAttributeString( id_step, i, STREAM_FIELD ) );
        snowflakeBulkLoaderFields[i].setTableField( rep.getStepAttributeString( id_step, i, TABLE_FIELD ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  /**
   * Saves the metadata for the step to a repository.
   *
   * @param rep               The repository
   * @param metaStore         The mestatore
   * @param id_transformation The ID of the transformation
   * @param id_step           The ID of the step
   * @throws KettleException
   */
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveDatabaseMetaStepAttribute( id_transformation, id_step, "id_connection", databaseMeta );
      rep.saveStepAttribute( id_transformation, id_step, TARGET_SCHEMA, targetSchema );
      rep.saveStepAttribute( id_transformation, id_step, TARGET_TABLE, targetTable );
      rep.saveStepAttribute( id_transformation, id_step, LOCATION_TYPE, locationType );
      rep.saveStepAttribute( id_transformation, id_step, STAGE_NAME, stageName );
      rep.saveStepAttribute( id_transformation, id_step, WORK_DIRECTORY, workDirectory );
      rep.saveStepAttribute( id_transformation, id_step, ON_ERROR, onError );
      rep.saveStepAttribute( id_transformation, id_step, ERROR_LIMIT, errorLimit );
      rep.saveStepAttribute( id_transformation, id_step, SPLIT_SIZE, splitSize );
      rep.saveStepAttribute( id_transformation, id_step, REMOVE_FILES, removeFiles );
      rep.saveStepAttribute( id_transformation, id_step, DATA_TYPE, dataType );
      rep.saveStepAttribute( id_transformation, id_step, TRIM_WHITESPACE, trimWhitespace );
      rep.saveStepAttribute( id_transformation, id_step, NULL_IF, nullIf );
      rep.saveStepAttribute( id_transformation, id_step, ERROR_COLUMN_MISMATCH, errorColumnMismatch );
      rep.saveStepAttribute( id_transformation, id_step, STRIP_NULL, stripNull );
      rep.saveStepAttribute( id_transformation, id_step, IGNORE_UTF_8, ignoreUtf8 );
      rep.saveStepAttribute( id_transformation, id_step, ALLOW_DUPLICATE_ELEMENT, allowDuplicateElements );
      rep.saveStepAttribute( id_transformation, id_step, ENABLE_OCTAL, enableOctal );
      rep.saveStepAttribute( id_transformation, id_step, SPECIFY_FIELDS, specifyFields );
      rep.saveStepAttribute( id_transformation, id_step, JSON_FIELD, jsonField );

      for ( int i = 0; i < snowflakeBulkLoaderFields.length; i++ ) {
        SnowflakeBulkLoaderField field = snowflakeBulkLoaderFields[i];

        rep.saveStepAttribute( id_transformation, id_step, i, STREAM_FIELD, field.getStreamField() );
        rep.saveStepAttribute( id_transformation, id_step, i, TABLE_FIELD, field.getTableField() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  /**
   * Check the step to make sure it is valid.  This is what is run when the user presses the check transformation
   * button in PDI
   * @param remarks The list of remarks to add to
   * @param transMeta The transformation metadata
   * @param stepMeta The step metadata
   * @param prev The metadata about the input stream
   * @param input The input fields
   * @param output The output fields
   * @param info The metadata about the info stream
   * @param space The variable space
   * @param repository The repository
   * @param metaStore The metastore
   */
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SnowflakeBulkLoadMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( SnowflakeBulkLoaderField snowflakeBulkLoaderField : snowflakeBulkLoaderFields ) {
        int idx = prev.indexOfValue( snowflakeBulkLoaderField.getStreamField() );
        if ( idx < 0 ) {
          error_message += "\t\t" + snowflakeBulkLoaderField.getStreamField() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message =
          BaseMessages.getString( PKG, "SnowflakeBulkLoadMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "SnowflakeBulkLoadMeta.CheckResult.AllFieldsFound" ), stepMeta );
        remarks.add( cr );
      }
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SnowflakeBulkLoadMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SnowflakeBulkLoadMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }

    for ( SnowflakeBulkLoaderField snowflakeBulkLoaderField : snowflakeBulkLoaderFields ) {
      try {
        snowflakeBulkLoaderField.validate();
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SnowflakeBulkLoadMeta.CheckResult.MappingValid", snowflakeBulkLoaderField.getStreamField() ),
          stepMeta );
        remarks.add( cr );
      } catch ( KettleException ex ) {
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SnowflakeBulkLoadMeta.CheckResult.MappingNotValid", snowflakeBulkLoaderField.getStreamField() ),
          stepMeta );
        remarks.add( cr );
      }
    }
  }

  /**
   * Gets a list of fields in the database table
   * @param space The variable space
   * @return The metadata about the fields in the table.
   * @throws KettleException
   */
  public RowMetaInterface getRequiredFields( VariableSpace space ) throws KettleException {
    String realTableName = space.environmentSubstitute( targetTable );
    String realSchemaName = space.environmentSubstitute( targetSchema );

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        if ( !Const.isEmpty( realTableName ) ) {
          String schemaTable = databaseMeta.getQuotedSchemaTableCombination( realSchemaName, realTableName );

          // Check if this table exists...
          if ( db.checkTableExists( schemaTable ) ) {
            return db.getTableFields( schemaTable );
          } else {
            throw new KettleException( BaseMessages.getString( PKG, "SnowflakeBulkLoaderMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString( PKG, "SnowflakeBulkLoaderMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new KettleException(
          BaseMessages.getString( PKG, "SnowflakeBulkLoaderMeta.Exception.ErrorGettingFields" ), e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new KettleException( BaseMessages.getString( PKG, "SnowflakeBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
    }

  }

  /**
   * Gets the list of databases used by the step
   * @return The list of databases used by the step
   */
  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[]{ databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /**
   * Gets the class that actually runs this step
   * @param stepMeta The metadata about the step
   * @param stepDataInterface The step data
   * @param cnr The step number
   * @param transMeta The metadata about the transformation
   * @param trans The transformation instance
   * @return The class that actually runs the step
   */
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new SnowflakeBulkLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  /**
   * Gets the step data
   * @return The step data
   */
  public StepDataInterface getStepData() {
    return new SnowflakeBulkLoaderData();
  }

  /**
   * Gets the Snowflake stage name based on the configured metadata
   * @param space The variable space
   * @return The Snowflake stage name to use
   */
  public String getStage( VariableSpace space ) {
    if ( locationType.equals( LOCATION_TYPE_CODES[LOCATION_TYPE_USER] ) ) {
      return "@~/" + space.environmentSubstitute( targetTable );
    } else if ( locationType.equals( LOCATION_TYPE_CODES[LOCATION_TYPE_TABLE] ) ) {
      if ( !Const.isEmpty( space.environmentSubstitute( targetSchema ) ) ) {
        return "@" + space.environmentSubstitute( targetSchema ) + ".%" + space.environmentSubstitute( targetTable );
      } else {
        return "@%" + space.environmentSubstitute( targetTable );
      }
    } else if ( locationType.equals( LOCATION_TYPE_CODES[LOCATION_TYPE_INTERNAL_STAGE] ) ) {
      if ( !Const.isEmpty( space.environmentSubstitute( targetSchema ) ) ) {
        return "@" + space.environmentSubstitute( targetSchema ) + "." + space.environmentSubstitute( stageName );
      } else {
        return "@" + space.environmentSubstitute( stageName );
      }
    }
    return null;
  }

  /**
   * Creates the copy statement used to load data into Snowflake
   * @param space The variable space
   * @param filenames A list of filenames to load
   * @return The copy statement to load data into Snowflake
   * @throws KettleFileException
   */
  public String getCopyStatement( VariableSpace space, List<String> filenames ) throws KettleFileException {
    StringBuilder returnValue = new StringBuilder();
    returnValue.append( "COPY INTO " );

    //Schema
    if ( !Const.isEmpty( space.environmentSubstitute( targetSchema ) ) ) {
      returnValue.append( space.environmentSubstitute( targetSchema ) ).append( "." );
    }

    //Table
    returnValue.append( space.environmentSubstitute( targetTable ) ).append( " " );

    // Location
    returnValue.append( "FROM " ).append( getStage( space ) ).append( "/ " );
    returnValue.append( "FILES = (" );
    boolean first = true;
    for ( String filename : filenames ) {
      String shortFile = KettleVFS.getFileObject( filename ).getName().getBaseName();
      if ( first ) {
        returnValue.append( "'" );
        first = false;
      } else {
        returnValue.append( ",'" );
      }
      returnValue.append( shortFile ).append( "' " );
    }
    returnValue.append( ") " );

    // FILE FORMAT
    returnValue.append( "FILE_FORMAT = ( TYPE = " );

    // CSV
    if ( dataType.equals( DATA_TYPE_CODES[DATA_TYPE_CSV] ) ) {
      returnValue.append( "'CSV' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\\n' ESCAPE = '\\\\' " );
      returnValue.append( "ESCAPE_UNENCLOSED_FIELD = '\\\\' FIELD_OPTIONALLY_ENCLOSED_BY='\"' " );
      returnValue.append( "SKIP_HEADER = 0 DATE_FORMAT = '" ).append( DATE_FORMAT_STRING ).append( "' " );
      returnValue.append( "TIMESTAMP_FORMAT = '" ).append( TIMESTAMP_FORMAT_STRING ).append( "' " );
      returnValue.append( "TRIM_SPACE = " ).append( trimWhitespace ).append( " " );
      if ( !Const.isEmpty( nullIf ) ) {
        returnValue.append( "NULL_IF = (" );
        String[] nullIfStrings = space.environmentSubstitute( nullIf ).split( "," );
        boolean firstNullIf = true;
        for ( String nullIfString : nullIfStrings ) {
          nullIfString = nullIfString.replaceAll( "'", "''" );
          if ( firstNullIf ) {
            firstNullIf = false;
            returnValue.append( "'" );
          } else {
            returnValue.append( ", '" );
          }
          returnValue.append( nullIfString ).append( "'" );
        }
        returnValue.append( " ) " );
      }
      returnValue.append( "ERROR_ON_COLUMN_COUNT_MISMATCH = " ).append( errorColumnMismatch ).append( " " );
      returnValue.append( "COMPRESSION = 'GZIP' " );

    } else if ( dataType.equals( DATA_TYPE_CODES[DATA_TYPE_JSON] ) ) {
      returnValue.append( "'JSON' COMPRESSION = 'GZIP' STRIP_OUTER_ARRAY = FALSE " );
      returnValue.append( "ENABLE_OCTAL = " ).append( enableOctal ).append( " " );
      returnValue.append( "ALLOW_DUPLICATE = " ).append( allowDuplicateElements ).append( " " );
      returnValue.append( "STRIP_NULL_VALUES = " ).append( stripNull ).append( " " );
      returnValue.append( "IGNORE_UTF8_ERRORS = " ).append( ignoreUtf8 ).append( " " );
    }
    returnValue.append( ") " );

    returnValue.append( "ON_ERROR = " );
    if ( onError.equals( ON_ERROR_CODES[ON_ERROR_ABORT] ) ) {
      returnValue.append( "'ABORT_STATEMENT' " );
    } else if ( onError.equals( ON_ERROR_CODES[ON_ERROR_CONTINUE] ) ) {
      returnValue.append( "'CONTINUE' " );
    } else if ( onError.equals( ON_ERROR_CODES[ON_ERROR_SKIP_FILE] )
      || onError.equals( ON_ERROR_CODES[ON_ERROR_SKIP_FILE_PERCENT] ) ) {
      if ( Const.toDouble( space.environmentSubstitute( errorLimit ), 0 ) <= 0 ) {
        returnValue.append( "'SKIP_FILE' " );
      } else {
        returnValue.append( "'SKIP_FILE_" ).append( Const.toInt( space.environmentSubstitute( errorLimit ), 0 ) );
      }
      if ( onError.equals( ON_ERROR_CODES[ON_ERROR_SKIP_FILE_PERCENT] ) ) {
        returnValue.append( "%' " );
      } else {
        returnValue.append( "' " );
      }
    }

    if( ! Boolean.getBoolean( space.environmentSubstitute( DEBUG_MODE_VAR ) ) ) {
      returnValue.append("PURGE = ").append(removeFiles).append(" ");
    }

    returnValue.append( ";" );

    return returnValue.toString();
  }

}
