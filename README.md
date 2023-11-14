Quick demo of using Athena as a data warehouse for some economic
data from FED FRED.

https://fred.stlouisfed.org/

> This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.


```sh
curl https://api.stlouisfed.org/fred/series/observations?series_id=GDPC1&api_key=$API_KEY&file_type=json
```


**Step functions input**
```json
{
  "series": [
    {"series_id": "GDPC1"},
    {"series_id": "MORTGAGE30US"},
    {"series_id": "UNRATE"}
  ]
}
```

