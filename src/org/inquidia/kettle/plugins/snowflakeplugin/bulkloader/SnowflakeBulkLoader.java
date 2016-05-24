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
import org.pentaho.di.core.exception.*;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
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

  @SuppressWarnings( "deprecation" )
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = ( SnowflakeBulkLoaderMeta ) smi;
    data = ( SnowflakeBulkLoaderData ) sdi;

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
        data.dbFields = meta.getRequiredFields();
        for ( int i = 0; i < meta.getSnowflakeBulkLoaderFields().length; i++ ) {
          int streamFieldLocation = data.outputRowMeta.indexOfValue(
            meta.getSnowflakeBulkLoaderFields()[i].getStreamField() );
          if ( streamFieldLocation < 0 ) {
            throw new KettleStepException( "Field [" + meta.getSnowflakeBulkLoaderFields()[i].getStreamField()
              + "] couldn't be found in the input stream!" );
          }

          int dbFieldLocation = data.dbFields.indexOfValue( meta.getSnowflakeBulkLoaderFields()[i].getTableField() );
          if ( dbFieldLocation < 0 ) {
            throw new KettleException( "Field [" + meta.getSnowflakeBulkLoaderFields()[i].getTableField()
              + "] couldn't be found in the table!" );
          }

          data.fieldnrs.put( meta.getSnowflakeBulkLoaderFields()[i].getTableField(), streamFieldLocation );
        }
      }

    }

    // Create a new split?
    if ( ( row != null && getLinesOutput() > 0 && SnowflakeBulkLoaderMeta.SPLIT_EVERY > 0
      && ( getLinesOutput() % SnowflakeBulkLoaderMeta.SPLIT_EVERY ) == 0 ) ) {

      // Done with this part or with everything.
      closeFile();

      // Not finished: open another file...
      openNewFile( buildFilename() );
    }

    if ( row == null ) {
      // no more input to be expected...
      loadDatabase();
      setOutputDone();
      return false;
    }

    writeRowToFile( data.outputRowMeta, row );
    putRow( data.outputRowMeta, row ); // in case we want it to go further...

    if ( checkFeedback( getLinesOutput() ) ) {
      logBasic( "linenr " + getLinesOutput() );
    }

    return true;
  }

  private void loadDatabase() throws KettleDatabaseException {
    for ( String file : data.getPreviouslyOpenedFiles() ) {
      String SQL = "PUT " + file + " " + meta.getStage( this );

      logDebug( "Executing SQL " + SQL );
      // data.db.execStatement( SQL );
    }

    String copySQL = meta.getCopyStatement( this, data.getPreviouslyOpenedFiles() );
    logDebug( "Executing SQL " + copySQL );
    // data.db.execStatement( copySQL );

  }

  private void writeRowToFile( RowMetaInterface rowMeta, Object[] r ) throws KettleStepException {
    try {
      if ( meta.getDataTypeId() == SnowflakeBulkLoaderMeta.DATA_TYPE_CSV || !meta.isSpecifyFields() ) {
        /*
         * Write all values in stream to text file.
         */
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }
          ValueMetaInterface v = rowMeta.getValueMeta( i );
          Object valueData = r[i];

          // no special null value default was specified since no fields are specified at all
          // As such, we pass null
          //
          writeField( v, valueData, null );
        }
        data.writer.write( data.binaryNewline );
      } else {
        /*
         * Only write the fields specified!
         */
        for ( int i = 0; i < data.dbFields.size(); i++ ) {
          if ( i > 0 && data.binarySeparator.length > 0 ) {
            data.writer.write( data.binarySeparator );
          }

          ValueMetaInterface v = data.dbFields.getValueMeta( i );

          switch ( v.getType() ) {
            case ValueMetaInterface.TYPE_TIMESTAMP:
              v.setConversionMask( SnowflakeBulkLoaderMeta.TIMESTAMP_FORMAT_STRING );
              break;
            case ValueMetaInterface.TYPE_DATE:
              v.setConversionMask( SnowflakeBulkLoaderMeta.DATE_FORMAT_STRING );
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              v.setConversionMask( "#" );
              break;
            case ValueMetaInterface.TYPE_BIGNUMBER:
            case ValueMetaInterface.TYPE_NUMBER:
              v.setConversionMask( "#.#" );
              break;
          }

          int fieldIndex = data.fieldnrs.get( data.dbFields.getFieldNames()[i] );
          Object valueData = null;
          if ( fieldIndex > 0 ) {
            valueData = v.convertData( rowMeta.getValueMeta( fieldIndex ), r[fieldIndex] );
          }
          writeField( v, valueData, data.binaryNullValue );
        }
        data.writer.write( data.binaryNewline );
      }

      incrementLinesOutput();

      // flush every 4k lines
      // if (linesOutput>0 && (linesOutput&0xFFF)==0) data.writer.flush();
    } catch ( Exception e ) {
      throw new KettleStepException( "Error writing line", e );
    }
  }

  private byte[] formatField( ValueMetaInterface v, Object valueData ) throws KettleValueException {
    if ( v.isString() ) {
      if ( v.isStorageBinaryString() && v.getTrimType() == ValueMetaInterface.TRIM_TYPE_NONE && v.getLength() < 0
        && Const.isEmpty( v.getStringEncoding() ) ) {
        return ( byte[] ) valueData;
      } else {
        String svalue = ( valueData instanceof String ) ? ( String ) valueData : v.getString( valueData );

        // trim or cut to size if needed.
        //
        return convertStringToBinaryString( v, Const.trimToType( svalue, v.getTrimType() ) );
      }
    } else {
      return v.getBinaryString( valueData );
    }
  }

  private byte[] convertStringToBinaryString( ValueMetaInterface v, String string ) throws KettleValueException {
    int length = v.getLength();

    if ( string == null ) {
      return new byte[] {};
    }

    if ( length > -1 && length < string.length() ) {
      // we need to truncate
      String tmp = string.substring( 0, length );
      if ( Const.isEmpty( v.getStringEncoding() ) ) {
        return tmp.getBytes();
      } else {
        try {
          return tmp.getBytes( v.getStringEncoding() );
        } catch ( UnsupportedEncodingException e ) {
          throw new KettleValueException( "Unable to convert String to Binary with specified string encoding ["
            + v.getStringEncoding() + "]", e );
        }
      }
    } else {
      byte[] text;
      if ( Const.isEmpty( v.getStringEncoding() ) ) {
        text = string.getBytes();
      } else {
        try {
          text = string.getBytes( v.getStringEncoding() );
        } catch ( UnsupportedEncodingException e ) {
          throw new KettleValueException( "Unable to convert String to Binary with specified string encoding ["
            + v.getStringEncoding() + "]", e );
        }
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

/*  private byte[] getBinaryString( String string ) throws KettleStepException {
    try {
      return string.getBytes( "UTF-8" );
    } catch ( Exception e ) {
      throw new KettleStepException( e );
    }
  } */

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
          if ( containsSeparatorOrEnclosure( str, data.binarySeparator, data.binaryEnclosure, data.escapeCharacters )
            ) {
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
            int position = enclosure;
            data.writer.write( str, from, position + data.escapeCharacters.length - from );
            data.writer.write( data.escapeCharacters ); // write enclosure a second time
            from = position + data.escapeCharacters.length;
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

  private List<Integer> getEnclosurePositions( byte[] str ) {
    List<Integer> positions = null;
    if ( data.binaryEnclosure != null && data.binaryEnclosure.length > 0 ) {
      // +1 because otherwise we will not find it at the end
      for ( int i = 0, len = str.length - data.binaryEnclosure.length + 1; i < len; i++ ) {
        // verify if on position i there is an enclosure
        //
        boolean found = true;
        for ( int x = 0; found && x < data.binaryEnclosure.length; x++ ) {
          if ( str[i + x] != data.binaryEnclosure[x] || str[i + x] != data.escapeCharacters[x] ) {
            found = false;
          }
        }
        if ( found ) {
          if ( positions == null ) {
            positions = new ArrayList<>();
          }
          positions.add( i );
        }
      }
    }
    return positions;
  }

  private String buildFilename() {
    return meta.buildFilename( this, getCopy(), getPartitionID(), data.splitnr );
  }

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

  private boolean checkPreviouslyOpened( String filename ) {

    return data.getPreviouslyOpenedFiles().contains( filename );

  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = ( SnowflakeBulkLoaderMeta ) smi;
    data = ( SnowflakeBulkLoaderData ) sdi;

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

  private void initBinaryDataFields() throws KettleException {
    try {
      data.binarySeparator = new byte[] {};
      data.binaryEnclosure = new byte[] {};
      data.binaryNewline = new byte[] {};
      data.escapeCharacters = new byte[] {};

      data.binarySeparator = environmentSubstitute( SnowflakeBulkLoaderMeta.CSV_DELIMITER ).getBytes( "UTF-8" );
      data.binaryEnclosure = environmentSubstitute( SnowflakeBulkLoaderMeta.ENCLOSURE ).getBytes( "UTF-8" );
      data.binaryNewline = SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes( "UTF-8" );
      data.escapeCharacters = SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes( "UTF-8" );

      data.binaryNullValue = "".getBytes();
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error while encoding binary fields", e );
    }
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = ( SnowflakeBulkLoaderMeta ) smi;
    data = ( SnowflakeBulkLoaderData ) sdi;

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

    super.dispose( smi, sdi );
  }

  @SuppressWarnings( "Duplicates" )
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


  @SuppressWarnings( "unused" )
  protected FileObject getFileObject( String vfsFilename ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename );
  }

  @SuppressWarnings( "unused" )
  protected FileObject getFileObject( String vfsFilename, VariableSpace space ) throws KettleFileException {
    return KettleVFS.getFileObject( vfsFilename, space );
  }

  private OutputStream getOutputStream( String vfsFilename, VariableSpace space, boolean append ) throws
    KettleFileException {
    return KettleVFS.getOutputStream( vfsFilename, space, append );
  }

}
