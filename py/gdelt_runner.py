from extractor import Extractor

if __name__ == "__main__":

    config = {
      "db_name": "gdelt_analysis",
      "db_user": "postgres",
      "db_pass": "postgres",
      "db_host": "localhost"
    }

    # Process a Year
    # This Assume the Table GDELT_2015 Already Exists
    for m in [i for i in range(1, 13)]:

        extractor = Extractor(config).run_month(1, 2015)
