import requests
import csv
from io import StringIO

def green_eco_roof():

    green_roof_url = "https://opendata.toronto.ca/toronto.building/building-permits-green-roofs/greenroofs.csv"
    green_roof_records = []
    with requests.get(green_roof_url, stream=True) as r:
        # decode bytes to string
        iterator = (line.decode("UTF-8") for line in r.iter_lines())

        buff = StringIO(next(iterator))
        header_reader = csv.reader(buff)
        for line in header_reader:
            source_headers = line
        reader = csv.DictReader(iterator, fieldnames = source_headers)
        
        for row in reader:
            green_roof_records.append(row)


    # loading ecoroof data
    import openpyxl
    from io import BytesIO
    import gc

    file_url = "https://opendata.toronto.ca/toronto.building/building-permits-green-roofs/Green Roof Permit Info_Open Data.xlsx"

    file = requests.get(file_url).content
    wb = openpyxl.load_workbook(filename = BytesIO(file))
    worksheet = wb["Sheet1"]

    source_headers = [col.value for col in worksheet[1]]

    print(source_headers)

    eco_roof_records = []

    for row in worksheet.iter_rows(min_row=2):
        output_row = {
            source_headers[i]: str(row[i].value).strip() if row[i].value else None
            for i in range(len(row))
        }

        eco_roof_records.append(output_row)


    for row in eco_roof_records:
        ef_permit_no_list = row['Building Permit Number'].split(' ') if row['Building Permit Number'] != "Unknown" else []
        ef_permit_no = ef_permit_no_list[0] + " " + ef_permit_no_list[1] if ef_permit_no_list else None

        row["permit_no_for_join"] = ef_permit_no


    # green roof left join eco roof
    outputs = []
    for green_row in green_roof_records:
        #print(f"green roof permit {green_row['PERMIT_NUM']}")
        
        for index in range(len(eco_roof_records)):
        
            if green_row['PERMIT_NUM'] == eco_roof_records[index]['permit_no_for_join']:
                print(f"Matched!!!!!!----------{green_row['PERMIT_NUM']}--------")
                green_row["Eco Roofs"] = True
                green_row["Year"] = eco_roof_records[index]["Year"]
                green_row["RefNo"] = eco_roof_records[index]["RefNo"]
                green_row["Eco roof address"] = eco_roof_records[index]["Eco roof address"]
                break
        
            else:
                green_row["Eco Roofs"] = False
                green_row["Year"] = None
                green_row["RefNo"] = None
                green_row["Eco roof address"] = None
        
        yield green_row


#keys = outputs[0].keys()

    
# with open('outputs.csv', 'w', newline='') as output_file:
#     dict_writer = csv.DictWriter(output_file, keys)
#     dict_writer.writeheader()
#     dict_writer.writerows(outputs)

outer = green_eco_roof()
print(next(outer))


