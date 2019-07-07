from .extractor import Extractor

if __name__ == "__main__":

    config = {
      "db_name": "gdelt_analysis",
      "db_user": "postgres",
      "db_pass": "postgres",
      "db_host": "localhost"
    }

    # Build a Year of Enriched GDELT Data
    # This Assumes the Table GDELT_2015 Already Exists

    for m in [i for i in range(1, 13)]:
        extractor = Extractor(config).run_month(m, 2015)
