/*******************************************************************************
 * Inquidia Consulting
 * <p>
 * Copyright (C) 2016 by Inquidia : http://www.inquidia.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.inquidia.kettle.plugins.snowflakeplugin.bulkloader;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.compress.CompressionProvider;
import org.pentaho.di.core.compress.CompressionProviderFactory;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaDate;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Bulk loads data to Snowflake
 */
@SuppressWarnings( { "UnusedAssignment", "ConstantConditions" } )
public class SnowflakeBulkLoader extends BaseStep implements StepInterface {
  private static Class<?> PKG = SnowflakeBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  private SnowflakeBulkLoaderMeta meta;

  private SnowflakeBulkLoaderData data;

  public SnowflakeBulkLoader( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                              Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  /**
   * Receive an input row from the stream, and write it to a local temp file.  After receiving the last row,
   * run the put and copy commands to copy the data into Snowflake.
   * @param smi The step metadata
   * @param sdi The step data
   * @return Was the row successfully processed.
   * @throws KettleException
   */
  @SuppressWarnings( "deprecation" )
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (SnowflakeBulkLoaderMeta) smi;
    data = (SnowflakeBulkLoaderData) sdi;

    Object[] row = getRow(); // This also waits for a row to be finished.

    if ( row != null && first ) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      // Open a new file here
      //
      openNewFile( buildFilename() );
      data.oneFileOpened = true;
      initBinaryDataFields();

      if ( meta.isSpecifyFields() && meta.getDataType().equals(
        SnowflakeBulkLoaderMeta.DATA_TYPE_CODES[SnowflakeBulkLoaderMeta.DATA_TYPE_CSV] ) ) {
        // Get input field mapping
        data.fieldnrs = new HashMap<>();
        getDbFields();
        for ( int i = 0; i < meta.getSnowflakeBulkLoaderFields().length; i++ ) {
          int streamFieldLocation = data.outputRowMeta.indexOfValue(
            meta.getSnowflakeBulkLoaderFields()[i].getStreamField() );
          if ( streamFieldLocation < 0 ) {
            throw new KettleStepException( "Field [" + meta.getSnowflakeBulkLoaderFields()[i].getStreamField()
              + "] couldn't be found in the input stream!" );
          }

          int dbFieldLocation = -1;
          for ( int e = 0; e < data.dbFields.size(); e++ ) {
            String[] field = data.dbFields.get( e );
            if ( field[0].equalsIgnoreCase( meta.getSnowflakeBulkLoaderFields()[i].getTableField() ) ) {
              dbFieldLocation = e;
              break;
            }
          }
          if ( dbFieldLocation < 0 ) {
            throw new KettleException( "Field [" + meta.getSnowflakeBulkLoaderFields()[i].getTableField()
              + "] couldn't be found in the table!" );
          }

          data.fieldnrs.put( meta.getSnowflakeBulkLoaderFields()[i].getTableField().toUpperCase(), streamFieldLocation );
        }
      } else if ( meta.getDataType().equals(
        SnowflakeBulkLoaderMeta.DATA_TYPE_CODES[SnowflakeBulkLoaderMeta.DATA_TYPE_JSON] ) ) {
        data.fieldnrs = new HashMap<>();
        int streamFieldLocation = data.outputRowMeta.indexOfValue(  meta.getJsonField() );
        if ( streamFieldLocation < 0 ) {
          throw new KettleStepException( "Field [" + meta.getJsonField()
            + "] couldn't be found in the input stream!" );
        }
        data.fieldnrs.put( "json", streamFieldLocation );
      }

    }

    // Create a new split?
    if ( ( row != null && data.outputCount > 0 && Const.toInt( environmentSubstitute( meta.getSplitSize() ), 0 ) > 0
      && ( data.outputCount % Const.toInt( environmentSubstitute( meta.getSplitSize() ), 0 ) ) == 0 ) ) {

      // Done with this part or with everything.
      closeFile();

      // Not finished: open another file...
      openNewFile( buildFilename() );
    }

    if ( row == null ) {
      // no more input to be expected...
      closeFile();
      loadDatabase();
      setOutputDone();
      return false;
    }

    writeRowToFile( data.outputRowMeta, row );
    putRow( data.outputRowMeta, row ); // in case we want it to go further...

    if ( checkFeedback( data.outputCount ) ) {
      logBasic( "linenr " + data.outputCount );
    }

    return true;
  }

  /**
   * Runs a desc table to get the fields, and field types from the database.  Uses a desc table as opposed
   * to the select * from table limit 0 that Pentaho normally uses to get the fields and types, due to the need
   * to handle the Time type.  The select * method through Pentaho does not give us the ability to differentiate
   * time from timestamp.
   * @throws KettleException
   */
  private void getDbFields() throws KettleException {
    data.dbFields = new ArrayList<>();
    String SQL = "desc table ";
    if ( !Const.isEmpty( environmentSubstitute( meta.getTargetSchema() ) ) ) {
      SQL += environmentSubstitute( meta.getTargetSchema() ) + ".";
    }
    SQL += environmentSubstitute( meta.getTargetTable() );
    logDetailed( "Executing SQL " + SQL );
    try {
      ResultSet resultSet = data.db.openQuery( SQL, null, null, ResultSet.FETCH_FORWARD, false );

      RowMetaInterface rowMeta = data.db.getReturnRowMeta();
      int nameField = rowMeta.indexOfValue( "NAME" );
      int typeField = rowMeta.indexOfValue( "TYPE" );
      if ( nameField < 0 || typeField < 0 ) {
        throw new KettleException( "Unable to get database fields" );
      }

      Object[] row = data.db.getRow( resultSet );
      if ( row == null ) {
        throw new KettleException( "No fields found in table" );
      }
      while ( row != null ) {
        String[] field = new String[2];
        field[0] = rowMeta.getString( row, nameField ).toUpperCase();
        field[1] = rowMeta.getString( row, typeField );
        data.dbFields.add( field );
        row = data.db.getRow( resultSet );
      }
      data.db.closeQuery( resultSet );
    } catch ( Exception ex ) {
      throw new KettleException( "Error getting database fields", ex );
    }
  }

  /**
   * Runs the commands to put the data to the Snowflake stage, the copy command to load the table, and finally
   * a commit to commit the transaction.
   * @throws KettleDatabaseException
   * @throws KettleFileException
   * @throws KettleValueException
   */
  private void loadDatabase() throws KettleDatabaseException, KettleFileException, KettleValueException {
    boolean filesUploaded = false;
    boolean endsWithSlash = environmentSubstitute( meta.getWorkDirectory() ).endsWith( "\\" )
      || environmentSubstitute( meta.getWorkDirectory() ).endsWith( "/" );
    String SQL = "PUT 'file://" + environmentSubstitute( meta.getWorkDirectory() ).replaceAll( "\\\\", "/" )
      + ( endsWithSlash ? "" : "/" ) + environmentSubstitute( meta.getTargetTable() ) + "_"
      + meta.getFileDate() + "_*' " + meta.getStage( this ) + ";";

    logDebug( "Executing SQL " + SQL );
    ResultSet putResultSet = data.db.openQuery( SQL, null, null, ResultSet.FETCH_FORWARD, false );
    RowMetaInterface putRowMeta = data.db.getReturnRowMeta();
    Object[] putRow = data.db.getRow( putResultSet );
    logDebug( "=========================Put File Results======================" );
    int fileNum = 0;
    while ( putRow != null ) {
      logDebug( "------------------------ File " + fileNum +"--------------------------" );
      for ( int i = 0; i < putRowMeta.getFieldNames().length; i++ ) {
        logDebug( putRowMeta.getFieldNames()[i] + " = " + putRowMeta.getString( putRow, i ) );
        if( putRowMeta.getFieldNames()[i].equalsIgnoreCase( "status" ) ) {
          if( putRowMeta.getString( putRow, i ).equalsIgnoreCase( "ERROR" ) ) {
            throw new KettleDatabaseException( "Error putting file to Snowflake stage \n" + putRowMeta.getString( putRow, "message", "" ) );
          }
        }
      }
      fileNum++;

      putRow = data.db.getRow( putResultSet );
    }
    data.db.closeQuery( putResultSet );

    String copySQL = meta.getCopyStatement( this, data.getPreviouslyOpenedFiles() );
    logDebug( "Executing SQL " + copySQL );
    ResultSet resultSet = data.db.openQuery( copySQL, null, null, ResultSet.FETCH_FORWARD, false );
    RowMetaInterface rowMeta = data.db.getReturnRowMeta();

    Object[] row = data.db.getRow( resultSet );
    int rowsLoaded = 0;
    int rowsLoadedField = rowMeta.indexOfValue( "rows_loaded" );
    int rowsError = 0;
    int errorField = rowMeta.indexOfValue( "errors_seen" );
    logBasic( "====================== Bulk Load Results======================" );
    int rowNum = 1;
    while ( row != null ) {
      logBasic( "---------------------- Row " + rowNum + " ----------------------" );
      for ( int i = 0; i < rowMeta.getFieldNames().length; i++ ) {
        logBasic( rowMeta.getFieldNames()[i] + " = " + rowMeta.getString( row, i ) );
      }

      if ( rowsLoadedField >= 0 ) {
        rowsLoaded += rowMeta.getInteger( row, rowsLoadedField );
      }

      if ( errorField >= 0 ) {
        rowsError += rowMeta.getInteger( row, errorField );
      }

      rowNum++;
      row = data.db.getRow( resultSet );
    }
    data.db.closeQuery( resultSet );
    setLinesOutput( rowsLoaded );
    setLinesRejected( rowsError );

    data.db.execStatement( "commit" );

  }

  /**
   * Writes an individual row of data to a temp file
   * @param rowMeta The metadata about the row
   * @param row The input row
   * @throws KettleStepException
   */
  private void writeRowToFile( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    try {
      if ( meta.getDataTypeId() == SnowflakeBulkLoaderMeta.DATA_TYPE_CSV && !meta.isSpecifyFields() ) {
        /*
         * Write all values in stream to text file.
         */
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }
          ValueMetaInterface v = rowMeta.getValueMeta( i );
          Object valueData = row[i];

          // no special null value default was specified since no fields are specified at all
          // As such, we pass null
          //
          writeField( v, valueData, null );
        }
        data.writer.write( data.binaryNewline );
      } else if ( meta.getDataTypeId() == SnowflakeBulkLoaderMeta.DATA_TYPE_CSV ) {
        /*
         * Only write the fields specified!
         */
        for ( int i = 0; i < data.dbFields.size(); i++ ) {
          if ( data.dbFields.get( i ) != null ) {
            if ( i > 0 && data.binarySeparator.length > 0 ) {
              data.writer.write( data.binarySeparator );
            }

            String[] field = data.dbFields.get( i );
            ValueMetaInterface v;

            if ( field[1].toUpperCase().startsWith( "TIMESTAMP" ) ) {
              v = new ValueMetaDate();
              v.setConversionMask( "yyyy-MM-dd HH:mm:ss.SSS" );
            } else if ( field[1].toUpperCase().startsWith( "DATE" ) ) {
              v = new ValueMetaDate();
              v.setConversionMask( "yyyy-MM-dd" );
            } else if ( field[1].toUpperCase().startsWith( "TIME" ) ) {
              v = new ValueMetaDate();
              v.setConversionMask( "HH:mm:ss.SSS" );
            } else if ( field[1].toUpperCase().startsWith( "NUMBER" ) || field[1].toUpperCase().startsWith( "FLOAT" ) ) {
              v = new ValueMetaBigNumber();
            } else {
              v = new ValueMetaString();
              v.setLength( -1 );
            }

            int fieldIndex = -1;
            if ( data.fieldnrs.get( data.dbFields.get( i )[0] ) != null ) {
              fieldIndex = data.fieldnrs.get( data.dbFields.get( i )[0] );
            }
            Object valueData = null;
            if ( fieldIndex >= 0 ) {
              valueData = v.convertData( rowMeta.getValueMeta( fieldIndex ), row[fieldIndex] );
            } else if ( meta.isErrorColumnMismatch() ) {
              throw new KettleException( "Error column mismatch: Database field " + data.dbFields.get( i )[0] + " not found on stream." );
            }
            writeField( v, valueData, data.binaryNullValue );
          }
        }
        data.writer.write( data.binaryNewline );
      } else {
        int jsonField = data.fieldnrs.get( "json" );
        data.writer.write( data.outputRowMeta.getString( row, jsonField ).getBytes( "UTF-8" ) );
        data.writer.write( data.binaryNewline );
      }

      data.outputCount++;

      // flush every 4k lines
      // if (linesOutput>0 && (linesOutput&0xFFF)==0) data.writer.flush();
    } catch ( Exception e ) {
      throw new KettleStepException( "Error writing line", e );
    }
  }

  /**
   * Takes an input field and converts it to bytes to be stored in the temp file.
   * @param v The metadata about the column
   * @param valueData The column data
   * @return The bytes for the value
   * @throws KettleValueException
   */
  private byte[] formatField( ValueMetaInterface v, Object valueData ) throws KettleValueException {
    if ( v.isString() ) {
      if ( v.isStorageBinaryString() && v.getTrimType() == ValueMetaInterface.TRIM_TYPE_NONE && v.getLength() < 0
        && Const.isEmpty( v.getStringEncoding() ) ) {
        return (byte[]) valueData;
      } else {
        String svalue = ( valueData instanceof String ) ? (String) valueData : v.getString( valueData );

        // trim or cut to size if needed.
        //
        return convertStringToBinaryString( v, Const.trimToType( svalue, v.getTrimType() ) );
      }
    } else {
      return v.getBinaryString( valueData );
    }
  }

  /**
   * Converts an input string to the bytes for the string
   * @param v The metadata about the column
   * @param string The column data
   * @return The bytes for the value
   * @throws KettleValueException
   */
  private byte[] convertStringToBinaryString( ValueMetaInterface v, String string ) throws KettleValueException {
    int length = v.getLength();

    if ( string == null ) {
      return new byte[]{};
    }

    if ( length > -1 && length < string.length() ) {
      // we need to truncate
      String tmp = string.substring( 0, length );
      try {
        return tmp.getBytes( "UTF-8" );
      } catch ( UnsupportedEncodingException e ) {
        throw new KettleValueException( "Unable to convert String to Binary with specified string encoding ["
            + v.getStringEncoding() + "]", e );
      }
    } else {
      byte[] text;
      try {
        text = string.getBytes( "UTF-8" );
      } catch ( UnsupportedEncodingException e ) {
        throw new KettleValueException( "Unable to convert String to Binary with specified string encoding ["
          + v.getStringEncoding() + "]", e );
      }

      if ( length > string.length() ) {
        // we need to pad this

        int size = 0;
        byte[] filler;
        try {
          filler = " ".getBytes( "UTF-8" );
          size = text.length + filler.length * ( length - string.length() );
        } catch ( UnsupportedEncodingException uee ) {
          throw new KettleValueException( uee );
        }
        byte[] bytes = new byte[size];
        System.arraycopy( text, 0, bytes, 0, text.length );
        if ( filler.length == 1 ) {
          java.util.Arrays.fill( bytes, text.length, size, filler[0] );
        } else {
          int currIndex = text.length;
          for ( int i = 0; i < ( length - string.length() ); i++ ) {
            for ( byte aFiller : filler ) {
              bytes[currIndex++] = aFiller;
            }
          }
        }
        return bytes;
      } else {
        // do not need to pad or truncate
        return text;
      }
    }
  }

  /**
   * Writes an individual field to the temp file.
   * @param v The metadata about the column
   * @param valueData The data for the column
   * @param nullString The bytes to put in the temp file if the value is null
   * @throws KettleStepException
   */
  private void writeField( ValueMetaInterface v, Object valueData, byte[] nullString ) throws KettleStepException {
    try {
      byte[] str;

      // First check whether or not we have a null string set
      // These values should be set when a null value passes
      //
      if ( nullString != null && v.isNull( valueData ) ) {
        str = nullString;
      } else {
        str = formatField( v, valueData );
      }

      if ( str != null && str.length > 0 ) {
        List<Integer> enclosures = null;
        boolean writeEnclosures = false;

        if ( v.isString() ) {
          if ( containsSeparatorOrEnclosure( str, data.binarySeparator, data.binaryEnclosure, data.escapeCharacters ) ) {
            writeEnclosures = true;
          }
        }

        if ( writeEnclosures ) {
          data.writer.write( data.binaryEnclosure );
          enclosures = getEnclosurePositions( str );
        }

        if ( enclosures == null ) {
          data.writer.write( str );
        } else {
          // Skip the enclosures, escape them instead...
          int from = 0;
          for ( Integer enclosure : enclosures ) {
            // Minus one to write the escape before the enclosure
            int position = enclosure;
            data.writer.write( str, from, position - from );
            data.writer.write( data.escapeCharacters ); // write enclosure a second time
            //data.writer.write( str, position, 1 );
            from = position;

          }
          if ( from < str.length ) {
            data.writer.write( str, from, str.length - from );
          }
        }

        if ( writeEnclosures ) {
          data.writer.write( data.binaryEnclosure );
        }
      }
    } catch ( Exception e ) {
      throw new KettleStepException( "Error writing field content to file", e );
    }
  }

  /**
   * Gets the positions of any double quotes or backslashes in the string
   * @param str The string to check
   * @return The positions within the string of double quotes and backslashes.
   */
  private List<Integer> getEnclosurePositions( byte[] str ) {
    List<Integer> positions = null;
    // +1 because otherwise we will not find it at the end
    for ( int i = 0, len = str.length; i < len; i++ ) {
      // verify if on position i there is an enclosure
      //
      boolean found = true;
      for ( int x = 0; found && x < data.binaryEnclosure.length; x++ ) {
        if ( str[i + x] != data.binaryEnclosure[x] ) {
          found = false;
        }
      }

      if ( !found ) {
        found = true;
        for ( int x = 0; found && x < data.escapeCharacters.length; x++ ) {
          if ( str[i + x] != data.escapeCharacters[x] ) {
            found = false;
          }
        }
      }

      if ( found ) {
        if ( positions == null ) {
          positions = new ArrayList<>();
        }
        positions.add( i );
      }
    }
    return positions;
  }

  /**
   * Get the filename to wrtie
   * @return The filename to use
   */
  private String buildFilename() {
    return meta.buildFilename( this, getCopy(), getPartitionID(), data.splitnr );
  }

  /**
   * Opens a file for writing
   * @param baseFilename The filename to write to
   * @throws KettleException
   */
  private void openNewFile( String baseFilename ) throws KettleException {
    if ( baseFilename == null ) {
      throw new KettleFileException( BaseMessages.getString( PKG, "SnowflakeBulkLoader.Exception.FileNameNotSet" ) );
    }

    data.writer = null;

    String filename = environmentSubstitute( baseFilename );

    try {
      CompressionProvider compressionProvider =
        CompressionProviderFactory.getInstance().getCompressionProviderByName( "GZip" );

      if ( compressionProvider == null ) {
        throw new KettleException( "No compression provider found with name = GZip" );
      }

      if ( !compressionProvider.supportsOutput() ) {
        throw new KettleException( "Compression provider GZip does not support output streams!" );
      }

      if ( log.isDetailed() ) {
        logDetailed( "Opening output stream using provider: " + compressionProvider.getName() );
      }

      if ( checkPreviouslyOpened( filename ) ) {
        data.fos = getOutputStream( filename, getTransMeta(), true );
      } else {
        data.fos = getOutputStream( filename, getTransMeta(), false );
        data.previouslyOpenedFiles.add( filename );
      }

      data.out = compressionProvider.createOutputStream( data.fos );

      // The compression output stream may also archive entries. For this we create the filename
      // (with appropriate extension) and add it as an entry to the output stream. For providers
      // that do not archive entries, they should use the default no-op implementation.
      data.out.addEntry( filename, "gz" );

      data.writer = new BufferedOutputStream( data.out, 5000 );

      if ( log.isDetailed() ) {
        logDetailed( "Opened new file with name ["
          + KettleVFS.getFriendlyURI( filename ) + "]" );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Error opening new file : " + e.toString() );
    }

    data.splitnr++;

  }

  /**
   * Closes a file so that its file handle is no longer open
   * @return true if we successfully closed the file
   */
  private boolean closeFile() {
    boolean returnValue = false;

    try {
      if ( data.writer != null ) {
        data.writer.flush();
      }
      data.writer = null;
      if ( log.isDebug() ) {
        logDebug( "Closing normal file ..." );
      }
      if ( data.out != null ) {
        data.out.close();
      }
      if ( data.fos != null ) {
        data.fos.close();
        data.fos = null;
      }
      returnValue = true;
    } catch ( Exception e ) {
      logError( "Exception trying to close file: " + e.toString() );
      setErrors( 1 );
      returnValue = false;
    }

    return returnValue;
  }

  /**
   * Checks if a filename was previously opened by the step
   * @param filename The filename to check
   * @return True if the step had previously opened the file
   */
  private boolean checkPreviouslyOpened( String filename ) {

    return data.getPreviouslyOpenedFiles().contains( filename );

  }

  /**
   * Initialize the step by connecting to the database and calculating some constants that will be used.
   * @param smi The step meta
   * @param sdi The step data
   * @return True if successfully initialized
   */
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (SnowflakeBulkLoaderMeta) smi;
    data = (SnowflakeBulkLoaderData) sdi;

    if ( super.init( smi, sdi ) ) {
      data.splitnr = 0;

      try {
        data.databaseMeta = meta.getDatabaseMeta();
/*        if ( !data.databaseMeta.getPluginId().equals( "SNOWFLAKE" ) ) {
          throw new KettleException( "Database is not a Snowflake database" );
        }
*/
        data.db = new Database( this, data.databaseMeta );
        data.db.shareVariablesWith( this );
        data.db.connect();

        if ( log.isBasic() ) {
          logBasic( "Connected to database [" + meta.getDatabaseMeta() + "]" );
        }

        data.db.setCommit( Integer.MAX_VALUE );

        initBinaryDataFields();
      } catch ( Exception e ) {
        logError( "Couldn't initialize binary data fields", e );
        setErrors( 1L );
        stopAll();
      }

      return true;
    }

    return false;
  }

  /**
   * Initialize the binary values of delimiters, enclosures, and escape characters
   * @throws KettleException
   */
  private void initBinaryDataFields() throws KettleException {
    try {
      data.binarySeparator = new byte[]{};
      data.binaryEnclosure = new byte[]{};
      data.binaryNewline = new byte[]{};
      data.escapeCharacters = new byte[]{};

      data.binarySeparator = environmentSubstitute( SnowflakeBulkLoaderMeta.CSV_DELIMITER ).getBytes( "UTF-8" );
      data.binaryEnclosure = environmentSubstitute( SnowflakeBulkLoaderMeta.ENCLOSURE ).getBytes( "UTF-8" );
      data.binaryNewline = SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes( "UTF-8" );
      data.escapeCharacters = SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes( "UTF-8" );

      data.binaryNullValue = "".getBytes( "UTF-8" );
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error while encoding binary fields", e );
    }
  }

  /**
   * Clean up after the step.  Close any open files, remove temp files, close any database connections.
   * @param smi The step metadata
   * @param sdi The step data
   */
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (SnowflakeBulkLoaderMeta) smi;
    data = (SnowflakeBulkLoaderData) sdi;

    if ( data.oneFileOpened ) {
      closeFile();
    }

    try {
      if ( data.fos != null ) {
        data.fos.close();
      }
    } catch ( Exception e ) {
      logError( "Unexpected error closing file", e );
      setErrors( 1 );
    }

    try {
      if ( data.db != null ) {
        data.db.disconnect();
      }
    } catch ( Exception e ) {
      logError( "Unable to close connection to database", e );
      setErrors( 1 );
    }

    if( ! Boolean.parseBoolean( environmentSubstitute( SnowflakeBulkLoaderMeta.DEBUG_MODE_VAR ) ) ) {
      for (String filename : data.previouslyOpenedFiles) {
        try {
          KettleVFS.getFileObject(filename).delete();
          logDetailed("Deleted temp file " + filename);
        } catch (Exception ex) {
          logMinimal("Unable to delete temp file", ex);
        }
      }
    }

    super.dispose( smi, sdi );
  }

  /**
   * Check if a string contains separators or enclosures.  Can be used to determine if the string
   * needs enclosures around it or not.
   * @param source The string to check
   * @param separator The separator character(s)
   * @param enclosure The enclosure character(s)
   * @param escape The escape character(s)
   * @return True if the string contains separators or enclosures
   */
  private boolean containsSeparatorOrEnclosure( byte[] source, byte[] separator, byte[] enclosure, byte[] escape ) {
    boolean result = false;

    boolean enclosureExists = enclosure != null && enclosure.length > 0;
    boolean separatorExists = separator != null && separator.length > 0;
    boolean escapeExists = escape != null && escape.length > 0;

    // Skip entire test if neither separator nor enclosure exist
    if ( separatorExists || enclosureExists || escapeExists ) {

      // Search for the first occurrence of the separator or enclosure
      for ( int index = 0; !result && index < source.length; index++ ) {
        if ( enclosureExists && source[index] == enclosure[0] ) {

          // Potential match found, make sure there are enough bytes to support a full match
          if ( index + enclosure.length <= source.length ) {
            // First byte of enclosure found
            result = true; // Assume match
            for ( int i = 1; i < enclosure.length; i++ ) {
              if ( source[index + i] != enclosure[i] ) {
                // Enclosure match is proven false
                result = false;
                break;
              }
            }
          }

        } else if ( separatorExists && source[index] == separator[0] ) {

          // Potential match found, make sure there are enough bytes to support a full match
          if ( index + separator.length <= source.length ) {
            // First byte of separator found
            result = true; // Assume match
            for ( int i = 1; i < separator.length; i++ ) {
              if ( source[index + i] != separator[i] ) {
                // Separator match is proven false
                result = false;
                break;
              }
            }
          }

        } else if ( escapeExists && source[index] == escape[0] ) {

          // Potential match found, make sure there are enough bytes to support a full match
          if ( index + escape.length <= source.length ) {
            // First byte of separator found
            result = true; // Assume match
            for ( int i = 1; i < escape.length; i++ ) {
              if ( source[index + i] != escape[i] ) {
                // Separator match is proven false
                result = false;
                break;
              }
            }
          }

        }
      }

    }

    return result;
  }


  /**
   * Gets a file handle
   * @param vfsFilename The file name
   * @return The file handle
   * @throws KettleFileException
   */
  @SuppressWarnings( "unused" )
  protected FileObject getFileObject( String vfsFilename ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename );
  }

  /**
   * Gets a file handle
   * @param vfsFilename The file name
   * @param space The variable space
   * @return The file handle
   * @throws KettleFileException
   */
  @SuppressWarnings( "unused" )
  protected FileObject getFileObject( String vfsFilename, VariableSpace space ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename, space );
  }

  /**
   * Gets the output stream to write to
   * @param vfsFilename The file name
   * @param space The variable space
   * @param append Should the file be appended
   * @return The output stream to write to
   * @throws KettleFileException
   */
  private OutputStream getOutputStream( String vfsFilename, VariableSpace space, boolean append ) throws
    KettleFileException {
    return KettleVFS.getOutputStream( vfsFilename, space, append );
  }

}
