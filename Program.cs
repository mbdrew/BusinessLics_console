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
using System.Net.Http;

[assembly: log4net.Config.XmlConfigurator(Watch =true)]

namespace BusinessLics_console
{
  class Program
  {
    private static readonly log4net.ILog log = log4net.LogManager.GetLogger("Program.cs");
    static void Main(string[] args)
    {

      /*
          First create a dictionary using business license number as keys and List<BusLicense> as values.  Use the city of LR REST service to build this.
          Loop thru each business object key in the dictionary:
	          query records for that license number in main table of database (this I call a rec set or "recs")
              If there is one business obj and <2 recs for that license number 
                Check if there are any recs for that busness license number, 
                  if not add to main tbl and changes tbl as ADD
                  if there is one, check if the records all match
                    If not, update rec in main table and add 2 recs to changes table as UPDATEOLD and UPDATENEW
              If there are >=2 business objs OR >=2 recs 
                Loop through each business object(s) and each rec for given license number and compare every field. 
                  If match is found for given business object, then skip
                  If no match is found for given business object, add that business object to main table and to changes table (as ADD).

            Loop through each rec in record set - this part is to determine the recs to delete. 
              See if there is a key in the dictionary with the license number in the record set.
                If there is no key, the license number must have been dropped.  Delete this record from main db table and add to changes tbl as DELETE.
                If there is a key, loop through the business object(s) in list and compare all fields.
                  If there is no match, delete this record from main db table and add to changes tbl as DELETE.
                
          The Program2 Main method is for creating a new LR_BusinessLicenses table.  This should only be done once except for during testing phase.
          To toggle between the programs, go to Properties --> Application tab --> startup object
        */

      string connStr = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;

      // Get the current business license data from the LR city website as list of business license objects. 

      // Had to add this because it was not working on the server. 
      System.Net.ServicePointManager.Expect100Continue = false;
      System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;

      //HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://data.littlerock.gov/resource/vthq-dt7e.json?license_type=RESTAURANT"));
      HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://data.littlerock.gov/resource/vthq-dt7e.json?$limit=50000"));

      WebReq.Method = "GET";

      HttpWebResponse WebResp = (HttpWebResponse)WebReq.GetResponse(); 

      int busLicCount = 0;
      int busLicDupCount = 0;
      int changedRecCount = 0;
      int addCount = 0;
      int deletedRecs = 0;
      Boolean matchFound = false;

      string jsonString;
      using (Stream stream = WebResp.GetResponseStream())
      {

        // Create a list of business objects from the LR city REST service. 

        StreamReader reader = new StreamReader(stream, System.Text.Encoding.UTF8);
        jsonString = reader.ReadToEnd();

        List<BusLicense> busLicenses = JsonConvert.DeserializeObject<List<BusLicense>>(jsonString);
        System.Diagnostics.Debug.WriteLine("busLicenses.Count= " + busLicenses.Count.ToString());

        if (busLicenses.Count < 1)
        {
          log.Debug("no business license objects returned from LR REST");
          return;
        }
        log.Debug("Total number of business licenses on LR site = " + busLicenses.Count);


        // Create a dictionary from list of BusLicense objects:  license number --> list of busLicense objects
        // Note: have to do this because there are duplicate records in LR City data with same business license number.
        // Only add the license numbers that start with BL

        Dictionary<string, List<BusLicense>> busLicenseDict = new Dictionary<string, List<BusLicense>>();
        foreach (var itm in busLicenses)
        {
          busLicCount = busLicCount + 1;
          if (itm.license_number.ToString().Substring(0, 2) == "BL")
          {
            if (busLicenseDict.ContainsKey(itm.license_number))
            {
              busLicDupCount = busLicDupCount + 1;
              busLicenseDict[itm.license_number].Add(itm);       
            }
            else
            {
              busLicenseDict.Add(itm.license_number, new List<BusLicense>());
              busLicenseDict[itm.license_number].Add(itm);
            }
          }
        }
        log.Debug("Total business licenses with BL prefix = " + busLicenseDict.Count().ToString());


        // Loop through the business license objects in the dictionary and use the license number to query the db table. 
        // If there is only one business object in the list and one or less recs, deal with these first. 

        foreach (var dictEntry in busLicenseDict)
        {

          string mainTblQry = "SELECT [license_number], [license_type], [license_classification], [license_category], [business_name], [contact_name], " +
                      "[account_start_date], [street_address], [city], [state], [zip_code], [x_coord], [y_coord] " +
                      "FROM [V8].[LRW_V8].[LR_BusinessLicenses] " +
                      "WHERE license_number = '" + dictEntry.Key + "'";
          BusinessLics_console.DataAccess da1 = new BusinessLics_console.DataAccess();
          DataSet ds2 = da1.GetData(mainTblQry);

          List<BusLicense> busLicenseList = busLicenseDict[dictEntry.Key];

          if (busLicenseList.Count == 1 && ds2.Tables[0].Rows.Count < 2)
          {

            if (ds2.Tables[0].Rows.Count < 1)
            {
              // There is no rec in main table, so add business license object to records in main and changes tables.  

              addCount = addCount + 1;
              InsertMainTbl(busLicenseList[0]);
              InsertChangesTbl(busLicenseList[0], "ADD");

            }
            else
            {
              DataRow dr = ds2.Tables[0].Rows[0];

              if (busLicenseList[0].license_type == (string)dr["license_type"]
                  && busLicenseList[0].license_classification == (string)dr["license_classification"]
                  && busLicenseList[0].license_category == (string)dr["license_category"]
                  && busLicenseList[0].business_name == (string)dr["business_name"]
                  && busLicenseList[0].contact_name == (string)dr["contact_name"]
                  && busLicenseList[0].account_start_date == (DateTime)dr["account_start_date"]
                  && busLicenseList[0].street_address == (string)dr["street_address"]
                  && busLicenseList[0].city == (string)dr["city"]
                  && busLicenseList[0].state == (string)dr["state"]
                  && busLicenseList[0].zip_code == (string)dr["zip_code"])
              {
                // Do nothing because there is a match.
              }
              else
              {
                // One or more field do not match, and there is only one business object and only one rec so must be an update.
                changedRecCount = changedRecCount + 1;

                deleteMainTbl(dictEntry.Key);
                InsertMainTbl(busLicenseList[0]);
                InsertChangesTbl(busLicenseList[0], "UPDATENEW");

                BusLicense blTemp = new BusLicense();
                blTemp.license_number = (string)dr["license_number"];
                blTemp.license_type = (string)dr["license_type"];
                blTemp.license_classification = (string)dr["license_classification"];
                blTemp.license_category = (string)dr["license_category"];
                blTemp.business_name = (string)dr["business_name"];
                blTemp.contact_name = (string)dr["contact_name"];
                blTemp.account_start_date = (DateTime)dr["account_start_date"];
                blTemp.street_address = (string)dr["street_address"];
                blTemp.city = (string)dr["city"];
                blTemp.state = (string)dr["state"];
                blTemp.zip_code = (string)dr["zip_code"];
                InsertChangesTbl(blTemp, "UPDATEOLD");
                blTemp = null;
              }
            }
          }
          else  // There is either >1 business objects or >1 recs in main table. 
          {

            foreach (var listEntry in dictEntry.Value)
            {

              BusLicense bl = new BusLicense();
              matchFound = false;
              foreach (DataRow dr in ds2.Tables[0].Rows)
              {
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

                if (bl.license_type == listEntry.license_type
                && bl.license_classification == listEntry.license_classification
                && bl.license_category == listEntry.license_category
                && bl.business_name == listEntry.business_name
                && bl.account_start_date == listEntry.account_start_date
                && bl.contact_name == listEntry.contact_name
                && bl.street_address == listEntry.street_address
                && bl.city == listEntry.city
                && bl.state == listEntry.state
                && bl.zip_code == listEntry.zip_code)
                {
                  // There is a match, so flip the switch
                  matchFound = true;
                }
                else
                {
                  // This was not a match so continue checking for matches
                }
              }
              if (matchFound == false)  // No matching recs found for this business object, so add it to main as ADD    
              {
                InsertMainTbl(listEntry);
                InsertChangesTbl(listEntry, "ADD");
              }
              bl = null;
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

        Boolean keyExists = true;
        Boolean matchExists = false;
        BusLicense bl_rec = new BusLicense();

        foreach (DataRow dr in dsAllRecs.Tables[0].Rows)
        {
          bl_rec.license_number = (string)dr["license_number"];
          bl_rec.license_type = (string)dr["license_type"];
          bl_rec.license_classification = (string)dr["license_classification"];
          bl_rec.license_category = (string)dr["license_category"];
          bl_rec.business_name = (string)dr["business_name"];
          bl_rec.contact_name = (string)dr["contact_name"];
          bl_rec.account_start_date = (DateTime)dr["account_start_date"];
          bl_rec.street_address = (string)dr["street_address"];
          bl_rec.city = (string)dr["city"];
          bl_rec.state = (string)dr["state"];
          bl_rec.zip_code = (string)dr["zip_code"];

          keyExists = busLicenseDict.ContainsKey((string)dr["license_number"]);
          if (keyExists == false)
          {
            InsertChangesTbl(bl_rec, "DELETE");
            deleteMainTbl((string)dr["license_number"]);
          }
          else
          {
            matchExists = false;
            foreach (BusLicense blo in busLicenseDict[(string)dr["license_number"]])  
            {
              if (blo.license_type == bl_rec.license_type 
                && blo.license_classification == bl_rec.license_classification 
                && blo.license_category == bl_rec.license_category 
                && blo.business_name == bl_rec.business_name 
                && blo.contact_name == bl_rec.contact_name 
                && blo.account_start_date == bl_rec.account_start_date 
                && blo.street_address == bl_rec.street_address 
                && blo.city == bl_rec.city 
                && blo.state == bl_rec.state 
                && blo.zip_code == bl_rec.zip_code)
              {
                matchExists = true;
              }
            }
            if (matchExists == false)
            {
              deletedRecs = deletedRecs + 1;
              InsertChangesTbl(bl_rec, "DELETE");
              deleteMainTbl2(bl_rec);
            }
          }
        }
      }

      log.Debug("Number of business licenses with duplicate entries = " + busLicDupCount.ToString());
      log.Debug("Number of records added = " + addCount.ToString());
      log.Debug("Number of updated records = " + changedRecCount.ToString());
      log.Debug("Number of deleted records = " + deletedRecs.ToString());
      log.Debug("program complete");
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

    public static void deleteMainTbl2(BusLicense busLic)
    {
      string connStr = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;
      string deleteQry = "DELETE FROM [LRW_V8].[LR_BusinessLicenses] WHERE license_number = @license_number AND license_type = @license_type AND license_classification = @license_classification " +
                         "AND license_category = @license_category AND contact_name = @contact_name AND account_start_date = @account_start_date AND street_address = @street_address " +
                         "AND city = @city AND state = @state AND zip_code = @zip_code";
      using (var conn = new SqlConnection(connStr))
      {
        using (SqlCommand command = new SqlCommand(deleteQry, conn))
        {
          command.Parameters.AddWithValue("@license_number", busLic.license_number);
          command.Parameters.AddWithValue("@license_type", busLic.license_type);
          command.Parameters.AddWithValue("@license_classification", busLic.license_classification);
          command.Parameters.AddWithValue("@license_category", busLic.license_category);
          command.Parameters.AddWithValue("@contact_name", busLic.contact_name);
          command.Parameters.AddWithValue("@account_start_date", busLic.account_start_date);
          command.Parameters.AddWithValue("@street_address", busLic.street_address);
          command.Parameters.AddWithValue("@city", busLic.city);
          command.Parameters.AddWithValue("@state", busLic.state);
          command.Parameters.AddWithValue("@zip_code", busLic.zip_code);

          conn.Open();
          int result = command.ExecuteNonQuery();
          if (result < 1)
            Debug.WriteLine("No data inserted");
          conn.Close();
        }
      }
    }

    #endregion Functions
  }


  class Program2
  {
    static void Main(string[] args)
    {

      HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://data.littlerock.gov/resource/vthq-dt7e.json?$limit=100000"));
      WebReq.Method = "GET";
      HttpWebResponse WebResp = (HttpWebResponse)WebReq.GetResponse();

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

        // Loop through the business license objects and add to table - add only the licenses that start with "BL"
        int addCount = 0;
        foreach (var itm in busLicenses)
        {

          if (itm.license_number.ToString().Substring(0, 2) == "BL")
          {
            addCount = addCount + 1;
            BusinessLics_console.Program.InsertMainTbl(itm);
          }
        }
        Debug.WriteLine("addCount = " + addCount.ToString());
        Debug.WriteLine("Program complete");
      }
    }
  }


}

