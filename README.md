# json2bq.py 參數說明

Var           | Value                                         | Description
:-------------|:----------------------------------------------|:------------------------
--project	  | gcp-expert-sandbox-allen	                  | 專案名稱
--region	  | us-central1	                                  | 地區名稱
--credential  | gcp-expert-sandbox-allen-c1fcfd19238a.json	  | 憑據檔案名稱
--temp_bucket | gcp-expert-sandbox-allen-temp_bucket	      | 暫存bucket名稱
--input	      | gs://gcp-expert-sandbox-allen/folder/data.json| 輸入的JSON位置
--output	  | gcp-expert-sandbox-allen:dataset.table	      | 輸出bq的表
--schema	  | '[{"name":"usage",<br>"type":"record",<br>"fields":<br>[{"name":"cpu",<br>"type":"STRING"},<br>{"name":"mem",<br>"type":"STRING"}]}]' | schema json
--skip_json_lines | 0                                       | Default: 0 , 忽略json的行數

# json2bq.py 使用說明

pip3 install -r requirements.txt

python3 json2bq.py --project=gcp-expert-sandbox-allen \\<br>
--region=us-central1 \\<br>
--credential=gcp-expert-sandbox-allen-c1fcfd19238a.json  \\<br>
--temp_bucket=gcp-expert-sandbox-allen-temp_bucket \\<br>
--input=gs://gcp-expert-sandbox-allen/folder/data.json \\<br>
--output=gcp-expert-sandbox-allen:dataset.table \\<br>
--schema="$(generate-schema < data.json)"