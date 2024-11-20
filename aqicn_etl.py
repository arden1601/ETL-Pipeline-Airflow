import requests as req

API = "62a42d9357c71ba0d49331a56c8acdf6ff343ef4"
cityid = "@8294"
url = f"https://api.waqi.info/feed/{cityid}/?token={API}"

response = req.get(url)

if response.status_code == 200:
    data = response.json()
    print(data["data"])
else:
    print("Failed to fetch data")
    
    
