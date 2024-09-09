from pymongo import MongoClient
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# MongoDB connection
mongo_uri = "mongodb://localhost:27017/"
client = MongoClient(mongo_uri)

# Specify the database and collection
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

# Aggregation pipeline
pipeline = [
    {
        "$match": {
            "HonorDistrict": {"$in": locations}
        }
    },
    {
        "$unwind": "$PurchaseOrderDetail"  # Unwind the PurchaseOrderDetail array
    },
    {
        "$match": {
            "PurchaseOrderDetail.GradeA": {"$gt": 0},
            "UserType": "Godown"
        }
    },
    {
        "$group": {
            "_id": "$HonorDistrict",  # Group by HonorDistrict
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
            "_id": 0,  # Exclude the _id field from the result
            "HonorDistrict": "$_id",
            "TotalGradeA": "$totalGradeA",
            "TotalProduct": "$totalProduct"
        }
    }
]

# Execute the aggregation pipeline
results = purchase_order_collection.aggregate(pipeline)

grand_total_gradeA = 0
grand_total_value = 0

# Google Sheets credentials
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
client_gspread = gspread.authorize(creds)

sheet = client_gspread.open("RSP weekly report").worksheet("CENTER_INV")


# Process and update individual rows
row_index = 2  # Start from the third row (B3)
for doc in results:
    value_in_L = round(doc['TotalProduct'] / 100000, 2)
    row = [
        [doc['HonorDistrict'], round(doc['TotalGradeA'], 2), value_in_L]
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

client.close()
