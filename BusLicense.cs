using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLics_console
{
  public class BusLicense
  {

    public string license_type { get; set; } = "";
    public string license_classification { get; set; } = "";
    public string license_category { get; set; } = "";
    public string license_number { get; set; }
    public string business_name { get; set; } = "";
    public string contact_name { get; set; } = "";
    public string street_address { get; set; } = "";
    public string city { get; set; } = "";
    public string state { get; set; } = "";
    public string zip_code { get; set; } = "";
    public DateTime account_start_date { get; set; } = Convert.ToDateTime("01-01-1000");

    public geocoded_col geocoded_column { get; set; }

    public class geocoded_col
    {
      public Decimal latitude { get; set; }
      public Decimal longitude { get; set; }
    }

  }
}
