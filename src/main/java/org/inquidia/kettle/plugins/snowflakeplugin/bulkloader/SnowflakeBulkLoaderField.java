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


import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.Injection;

/**
 * Describes a single field mapping from the Pentaho stream to the Snowflake table
 */
public class SnowflakeBulkLoaderField implements Cloneable {

  /** The field name on the stream */
  @Injection( name = "STREAM_FIELD", group = "OUTPUT_FIELDS" )
  private String streamField;

  /** The field name on the table */
  @Injection( name = "TABLE_FIELD", group = "OUTPUT_FIELDS" )
  private String tableField;

  /**
   *
   * @param streamField The name of the stream field
   * @param tableField The name of the field on the table
   */
  public SnowflakeBulkLoaderField( String streamField, String tableField ) {
    this.streamField = streamField;
    this.tableField = tableField;
  }

  public SnowflakeBulkLoaderField() {
  }

  /**
   * Enables deep cloning
   * @return A new instance of SnowflakeBulkLoaderField
   */
  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * Validate that the SnowflakeBulkLoaderField is good
   * @return
   * @throws KettleException
   */
  public boolean validate() throws KettleException {
    if ( streamField == null || tableField == null ) {
      throw new KettleException( "Validation error: Both stream field and database field must be populated." );
    }

    return true;
  }

  /**
   *
   * @return The name of the stream field
   */
  public String getStreamField() {
    return streamField;
  }

  /**
   * Set the stream field
   * @param streamField The name of the field on the Pentaho stream
   */
  public void setStreamField( String streamField ) {
    this.streamField = streamField;
  }

  /**
   *
   * @return The name of the field in the Snowflake table
   */
  public String getTableField() {
    return tableField;
  }

  /**
   * Set the field in the Snowflake table
   * @param tableField The name of the field on the table
   */
  public void setTableField( String tableField ) {
    this.tableField = tableField;
  }

  /**
   *
   * @return A string in the "streamField -> tableField" format
   */
  public String toString() {
    return streamField + " -> " + tableField;
  }


}
