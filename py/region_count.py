import arcpy  # Python 3
import json


if __name__ == "__main__":

    mg = "C:\\Users\\jeff8977\\Desktop\\GDELT Research\\GDELT Research.gdb\\GDELT_HATE_DD_Keys"
    dv = "C:\\Users\\jeff8977\\Desktop\\GDELT Research\\GDELT Research.gdb\\US_States_2018"

    with open('../planning/gdelt_hate.json') as config:
        param_dict = json.load(config)

    with arcpy.da.SearchCursor(mg, ['OBJECTID', 'START_DATE', 'END_DATE', 'SHAPE@']) as cursor:
        for row in cursor:

            er_id = '_'.join([row[1].strftime('%Y%m%d') ,row[2].strftime('%Y%m%d')])
            print(f'Running: {er_id}')

            sel = arcpy.SelectLayerByAttribute_management(
                mg,
                "NEW_SELECTION",
                f"OBJECTID = {row[0]}"
            )

            loc = arcpy.SelectLayerByLocation_management(
                dv,
                "HAVE_THEIR_CENTER_IN",
                sel
            )
            cnt = arcpy.GetCount_management(loc)

            param_dict[er_id].update({'extent': cnt[0], 'area': round(row[3].area)})

    with open('../planning/gdelt_hate.json', 'w') as the_file:
        json.dump(param_dict, the_file, indent=2)
