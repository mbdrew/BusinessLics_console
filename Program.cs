using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using Newtonsoft.Json;
using System.Net;
using System.IO;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Configuration;

namespace BusinessLics_console
{
  class Program
  {
    static void Main(string[] args)
    {
      string connStr = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;
      

      /*
        * First create a dictionary using business license number as keys and List<BusLicense> as values.  Use the city of LR REST service to build this.
        * Then loop through the dictionary.  For each business license object, query the main table (LR_BusinessLicenses) to get rec(s) with that license number.
        * For each business object:
        *   Check if there is a matching license number in the main table (use SQL query and put result into record set - could be >1).
        *     If there is not, then use the object to add a rec to the main table and to the changes table (as ADD).
        *     If there is, check each rec in main table and test if all fields match.
        *       If there is an all field match, continue
        *       If there is not an all field match, then
        *         If there is only one object in dictionary and only one rec in main table then UPDATE main table rec and add to changes table (as UPDATE).
        *         If are more than one object or rec, don't know which rec to update so add new rec to main table and add to changes table (as ADD). 
        * Create record set of whole main table (LR_BusinessLicenses).
        *   Loop through each rec in record set
        *     See if there is a key in the dictionary with the license number in the record set.
        *       If there is no key, the license number must have been dropped.  Delete this record from main db table and add to changes tbl as DELETE.
        */


      // Get the current business license data from the LR city website as list of business license objects. 

      //HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://data.littlerock.gov/resource/vthq-dt7e.json?license_type=RESTAURANT"));
      HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://data.littlerock.gov/resource/vthq-dt7e.json?$limit=100000"));
      WebReq.Method = "GET";
      HttpWebResponse WebResp = (HttpWebResponse)WebReq.GetResponse();

      int busLicCount = 0;
      int busLicDupCount = 0;
      int changedRecCount = 0;
      int addCount = 0;
      Boolean matchFound = false;

      string jsonString;
      using (Stream stream = WebResp.GetResponseStream())
      {
        StreamReader reader = new StreamReader(stream, System.Text.Encoding.UTF8);
        jsonString = reader.ReadToEnd();

        List<BusLicense> busLicenses = JsonConvert.DeserializeObject<List<BusLicense>>(jsonString);
        System.Diagnostics.Debug.WriteLine("busLicenses.Count= " + busLicenses.Count.ToString());

        if (busLicenses.Count < 1)
        {
          Console.WriteLine("no business license objects returned from LR REST");
          return;
        }


        // Create a dictionary from list of BusLicense objects:  license number --> list of busLicense objects
        // Note: have to do this because there are duplicate records in LR City data with same business license number.
        // Only add the license numbers that start with BL

        Dictionary<string, List<BusLicense>> busLicenseDict = new Dictionary<string, List<BusLicense>>();
        foreach (var itm in busLicenses)
        {

          //if (itm.license_type.ToString() == "PEDDLERS")
          //{
          //  Debug.WriteLine("do not add to diectionary: " + itm.license_number.ToString());
          //  continue;
          //}

          busLicCount = busLicCount + 1;
          if (itm.license_number.ToString().Substring(0, 2) == "BL")
          {
            if (busLicenseDict.ContainsKey(itm.license_number))
            {
              busLicDupCount = busLicDupCount + 1;
              if (itm.license_number == "BL133119")
              {
                itm.license_type = "DREW";
              }
              busLicenseDict[itm.license_number].Add(itm);
              Debug.WriteLine("This is a duplicate: " + itm.license_number);
            }
            else
            {
              busLicenseDict.Add(itm.license_number, new List<BusLicense>());
              busLicenseDict[itm.license_number].Add(itm);
            }
          }
        }
        Debug.WriteLine("busLicenseDict.Count = " + busLicenseDict.Count().ToString());

        // Loop through the business license object in the dictionary and use the license number to query the db table. 
        // first check if there is an entry for this license number in the main db table, if not use info in object to add a rec to main and changes table.
        // If there is an entry (or entries) for this license number in the main db table, see if all fields match.  If so, skip.  If not, add rec to main 
        // and changes tbls.

        foreach (var dictEntry in busLicenseDict)
        {

          string mainTblQry = "SELECT [license_number], [license_type], [license_classification], [license_category], [business_name], [contact_name], " +
                      "[account_start_date], [street_address], [city], [state], [zip_code], [x_coord], [y_coord] " +
                      "FROM [V8].[LRW_V8].[LR_BusinessLicenses] " +
                      "WHERE license_number = '" + dictEntry.Key + "'";
          BusinessLics_console.DataAccess da1 = new BusinessLics_console.DataAccess();
          DataSet ds2 = da1.GetData(mainTblQry);

          if (ds2.Tables[0].Rows.Count < 1)
          {
            // Add business license objects to records in main and changes tables.  Have to use foreach loop in case there are more than one. 
            addCount = addCount + 1;
            Debug.WriteLine("This bus lic does not exist in db: " + dictEntry.Key);
            foreach (var listEntry in dictEntry.Value)
            {
              InsertMainTbl(listEntry);
              InsertChangesTbl(listEntry, "ADD");
            }
          }

          else
          {
            // for each license object in the list (usually only one), check to see if there is rec in db table with matching fields. 
            foreach (var listEntry in dictEntry.Value)
            {
              matchFound = false;
              foreach (DataRow dr in ds2.Tables[0].Rows)
              {
                if ((string)dr["license_type"] == listEntry.license_type
                && (string)dr["license_classification"] == listEntry.license_classification
                && (string)dr["license_category"] == listEntry.license_category
                && (string)dr["business_name"] == listEntry.business_name
                //&& (DateTime)ds2.Tables[0].Rows[0]["account_start_date"] == itm.account_start_date
                && (string)dr["contact_name"] == listEntry.contact_name
                && (string)dr["street_address"] == listEntry.street_address
                && (string)dr["city"] == listEntry.city
                && (string)dr["state"] == listEntry.state
                && (string)dr["zip_code"] == listEntry.zip_code)
                {
                  // There is a match, so flip the switch
                  matchFound = true;
                }
                else
                {
                  // This was not a match so continue 
                }
              }
              if (matchFound == false)  // No matching recs found.  If there is only one obj in dictionary and one rec in db table, then this must be an update.  
              {
                if (dictEntry.Value.Count == 1 && ds2.Tables[0].Rows.Count == 1)
                {
                  changedRecCount = changedRecCount + 1;
                  InsertMainTbl(listEntry);
                  InsertChangesTbl(listEntry, "UPDATE");
                }
                else
                {
                  InsertMainTbl(listEntry);
                  InsertChangesTbl(listEntry, "ADD");
                }
              }
            }
          }
        }  // Ends foreach loop for business object dictionary


        // Loop through the main table in database to see what recs should be deleted.  If a rec in main table does not have matching license number
        // in the dictionary, then delete it from main table and add a rec to changes table as DELETE.  

        string allRecsQry = "SELECT [license_number], [license_type], [license_classification], [license_category], [business_name], [contact_name], " +
            "[account_start_date], [street_address], [city], [state], [zip_code], [x_coord], [y_coord] " +
            "FROM [V8].[LRW_V8].[LR_BusinessLicenses] ";
        BusinessLics_console.DataAccess daAllRecs = new BusinessLics_console.DataAccess();
        DataSet dsAllRecs = daAllRecs.GetData(allRecsQry);

        Boolean keyExists;
        foreach (DataRow dr in dsAllRecs.Tables[0].Rows)
        {
          keyExists = busLicenseDict.ContainsKey((string)dr["license_number"]);
          if (keyExists == false)
          {
            Debug.WriteLine("This license not in dictionary and will be deleted: " + (string)dr["license_number"]);

            BusLicense bl = new BusLicense();
            bl.license_number = (string)dr["license_number"];
            bl.license_type = (string)dr["license_type"];
            bl.license_classification = (string)dr["license_classification"];
            bl.license_category = (string)dr["license_category"];
            bl.business_name = (string)dr["business_name"];
            bl.contact_name = (string)dr["contact_name"];
            bl.account_start_date = (DateTime)dr["account_start_date"];
            bl.street_address = (string)dr["street_address"];
            bl.city = (string)dr["city"];
            bl.state = (string)dr["state"];
            bl.zip_code = (string)dr["zip_code"];

            InsertChangesTbl(bl, "DELETE");
            deleteMainTbl((string)dr["license_number"]);
            bl = null;
          }
        }
      }

      Debug.WriteLine("busLicCount = " + busLicCount.ToString());
      Debug.WriteLine("busLicDupCount = " + busLicDupCount.ToString());
      Debug.WriteLine("The AddCount is: " + addCount.ToString());
      Debug.WriteLine("changedRecCount = " + changedRecCount.ToString());
      Debug.WriteLine("program complete");

    }

    #region Functions

    public static void InsertChangesTbl(BusLicense busLic, string action)
    {
      string connStr = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;

      string insertChangesQry = "INSERT INTO [LRW_V8].[LR_BusinessLicenses_Changes] (RUNDATE, ACTION, license_number, license_type, license_classification, license_category, business_name, contact_name, account_start_date, street_address, city, state, zip_code) " +
                                                       "VALUES (@RUNDATE, @ACTION, @license_number, @license_type, @license_classification, @license_category, @business_name, @contact_name, @account_start_date, @street_address, @city, @state, @zip_code)";

      using (var conn = new SqlConnection(connStr))
      {
        using (SqlCommand command2 = new SqlCommand(insertChangesQry, conn))
        {
          command2.Parameters.AddWithValue("@RUNDATE", DateTime.Now);
          command2.Parameters.AddWithValue("@ACTION", action);
          command2.Parameters.AddWithValue("@license_number", (string)busLic.license_number);
          command2.Parameters.AddWithValue("@license_type", (string)busLic.license_type);
          command2.Parameters.AddWithValue("@license_classification", (string)busLic.license_classification);
          command2.Parameters.AddWithValue("@license_category", (string)busLic.license_category);
          command2.Parameters.AddWithValue("@business_name", (string)busLic.business_name);
          command2.Parameters.AddWithValue("@contact_name", (string)busLic.contact_name);
          command2.Parameters.AddWithValue("@account_start_date", busLic.account_start_date);
          command2.Parameters.AddWithValue("@street_address", (string)busLic.street_address);
          command2.Parameters.AddWithValue("@city", (string)busLic.city);
          command2.Parameters.AddWithValue("@state", (string)busLic.state);
          command2.Parameters.AddWithValue("@zip_code", (string)busLic.zip_code);
          //command2.Parameters.AddWithValue("@x_coord", Convert.ToDouble(busLic.geocoded_column.longitude));
          //command2.Parameters.AddWithValue("@y_coord", Convert.ToDouble(busLic.geocoded_column.latitude));

          conn.Open();
          int result = command2.ExecuteNonQuery();
          if (result < 1)
            Debug.WriteLine("No data inserted");
          conn.Close();
        }
      }
    }

    public static void InsertMainTbl(BusLicense busLic)
    {
      string connStr = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;
      string insertQry = "INSERT INTO [LRW_V8].[LR_BusinessLicenses] (license_number, license_type, license_classification, license_category, business_name, contact_name, account_start_date, street_address, city, state, zip_code, x_coord, y_coord) " +
                                                       "VALUES (@license_number, @license_type, @license_classification, @license_category, @business_name, @contact_name, @account_start_date, @street_address, @city, @state, @zip_code, @x_coord, @y_coord)";

      using (var conn = new SqlConnection(connStr))
      {
        //  Insert into the LR_BusinessLicenses table
        //Debug.WriteLine("Adding rec using InsertMainTable.  license_number = " + busLic.license_number);
        using (SqlCommand command = new SqlCommand(insertQry, conn))
        {
          command.Parameters.AddWithValue("@license_number", (string)busLic.license_number);
          command.Parameters.AddWithValue("@license_type", (string)busLic.license_type);
          command.Parameters.AddWithValue("@license_classification", (string)busLic.license_classification);
          command.Parameters.AddWithValue("@license_category", (string)busLic.license_category);
          command.Parameters.AddWithValue("@business_name", (string)busLic.business_name);
          command.Parameters.AddWithValue("@contact_name", (string)busLic.contact_name);
          command.Parameters.AddWithValue("@account_start_date", busLic.account_start_date);
          command.Parameters.AddWithValue("@street_address", (string)busLic.street_address);
          command.Parameters.AddWithValue("@city", (string)busLic.city);
          command.Parameters.AddWithValue("@state", (string)busLic.state);
          command.Parameters.AddWithValue("@zip_code", (string)busLic.zip_code);
          command.Parameters.AddWithValue("@x_coord", Convert.ToDouble(busLic.geocoded_column.longitude));
          command.Parameters.AddWithValue("@y_coord", Convert.ToDouble(busLic.geocoded_column.latitude));

          conn.Open();
          int result = command.ExecuteNonQuery();
          if (result < 1)
            Debug.WriteLine("No data inserted");
          conn.Close();
        }
      }
    }

    public static void deleteMainTbl(string licenseNum)
    {
      string connStr = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;
      string deleteQry = "DELETE FROM [LRW_V8].[LR_BusinessLicenses] WHERE license_number = @license_number";
      using (var conn = new SqlConnection(connStr))
      {
        using (SqlCommand command = new SqlCommand(deleteQry, conn))
        {
          command.Parameters.AddWithValue("@license_number", licenseNum);

          conn.Open();
          int result = command.ExecuteNonQuery();
          if (result < 1)
            Debug.WriteLine("No data inserted");
          conn.Close();
        }
      }
    }
  }


}
#endregion Functions
