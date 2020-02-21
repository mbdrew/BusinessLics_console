using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;

namespace BusinessLics_console
{
  public class DataAccess
  {
    public DataSet GetData(string query)
    {

      string conString = ConfigurationManager.ConnectionStrings["IMSReader"].ConnectionString;
      SqlCommand cmd = new SqlCommand(query);
      using (SqlConnection con = new SqlConnection(conString))
      {
        using (SqlDataAdapter sda = new SqlDataAdapter())
        {
          cmd.Connection = con;
          sda.SelectCommand = cmd;
          using (DataSet ds = new DataSet())
          {
            sda.Fill(ds);
            return ds;
          }
        }
      }
    }
  }
}
