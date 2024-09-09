from pymongo import MongoClient
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

mongo_uri = "mongodb://localhost:27017/"
client = MongoClient(mongo_uri)

db = client['test']
purchase_order_collection = db['tblPurchaseOrder']

locations = [
    "Waranga phata",
    "Narsiphata",
    "Hiremyagiri",
    "Ghungrala",
    "Naregal",
    "Sovenahalli",
    "Kadampur",
    "Borgaon",
    "Bhokar phata",
    "Khanapur",
    "Sasavehalli",
    "Ittagi",
    "Mehkar",
    "Arsikere",
    "Farthabhad",
    "Siddeshwar"
]

pipeline = [
    {
        "$match": {
            "HonorDistrict": {"$in": locations}
        }
    },
    {
        "$unwind": "$PurchaseOrderDetail"
    },
    {
        "$match": {
            "PurchaseOrderDetail.GradeA": {"$gt": 0},
            "UserType": "Godown"
        }
    },
    {
        "$group": {
            "_id": "$PurchaseOrderDetail.CropName",
            "totalGradeA": {"$sum": "$PurchaseOrderDetail.GradeA"},
            "totalProduct": {
                "$sum": {
                    "$multiply": ["$PurchaseOrderDetail.GradeA", "$PurchaseOrderDetail.GradeAAvgPrice"]
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "CropName": "$_id",
            "TotalGradeA": "$totalGradeA",
            "TotalProduct": "$totalProduct"
        }
    }
]

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
client_gspread = gspread.authorize(creds)

sheet = client_gspread.open("RSP weekly report").worksheet("CROP_INV")

results = purchase_order_collection.aggregate(pipeline)

grand_total_gradeA = 0
grand_total_value = 0
row_index = 2
for doc in results:
    print(doc)
    value_in_L = round(doc['TotalProduct'] / 100000, 2)
    row = [
        [doc['CropName'], round(doc['TotalGradeA'], 2), value_in_L]
    ]
    range_name = f'A{row_index}:C{row_index}'
    sheet.update(range_name, row)
    row_index += 1
    
    # Update grand totals
    grand_total_gradeA += round(doc['TotalGradeA'], 2)
    grand_total_value += value_in_L

grand_total_text = "Grand Total"
row_total = [
    [grand_total_text, grand_total_gradeA, grand_total_value]
]
sheet.update(f'A{row_index}:C{row_index}', row_total)
print("\nGrand Total:")
print(f"Total GradeA: {round(grand_total_gradeA,2)}, Total Product (in lakhs): {round(grand_total_value,2)}")

client.close()
