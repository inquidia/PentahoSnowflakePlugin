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

import org.pentaho.di.core.compress.CompressionOutputStream;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.io.OutputStream;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Inquidia Consulting
 */
@SuppressWarnings( "WeakerAccess" )
public class SnowflakeBulkLoaderData extends BaseStepData implements StepDataInterface {

  public int splitnr;

  public Map<String, Integer> fieldnrs;

  public Database db;
  public DatabaseMeta databaseMeta;

  public ArrayList<String[]> dbFields;

  public int outputCount;


  public NumberFormat nf;
  public DecimalFormat df;
  public DecimalFormatSymbols dfs;

  public SimpleDateFormat daf;
  public DateFormatSymbols dafs;

  public CompressionOutputStream out;

  public OutputStream writer;

  public DecimalFormat defaultDecimalFormat;
  public DecimalFormatSymbols defaultDecimalFormatSymbols;

  public SimpleDateFormat defaultDateFormat;
  public DateFormatSymbols defaultDateFormatSymbols;

  public OutputStream fos;

  public RowMetaInterface outputRowMeta;

  public byte[] binarySeparator;
  public byte[] binaryEnclosure;
  public byte[] escapeCharacters;
  public byte[] binaryNewline;

  public byte[] binaryNullValue;

  public boolean oneFileOpened;

  public List<String> previouslyOpenedFiles;

  public int fileNameFieldIndex;

  public Map<String, OutputStream> fileWriterMap;

  public SnowflakeBulkLoaderData() {
    super();

    nf = NumberFormat.getInstance();
    df = (DecimalFormat) nf;
    dfs = new DecimalFormatSymbols();

    daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    defaultDecimalFormat = (DecimalFormat) NumberFormat.getInstance();
    defaultDecimalFormatSymbols = new DecimalFormatSymbols();

    defaultDateFormat = new SimpleDateFormat();
    defaultDateFormatSymbols = new DateFormatSymbols();

    previouslyOpenedFiles = new ArrayList<>();
    fileNameFieldIndex = -1;

    oneFileOpened = false;
    outputCount = 0;

    fileWriterMap = new HashMap<>();

    dbFields = null;
    db = null;
  }

  List<String> getPreviouslyOpenedFiles() {
    return previouslyOpenedFiles;
  }
}
