# Recommendation engine based on primary interests and clicks 
Executable file - `primary-interests-events-mark.py`  
Creates weight matrix with :  
* Rows - users 
* Columns - categories  
* Values - weight of category  

Start params  
```bash
python3 primary_interests_events_mark.py --help
```  

DS-instance execution path `/home/ubuntu/recommendation-system/primary_interests_events_mark.py`

Generate proper version of categories shortlist(will be both saved localy and to DS instance). Start params:
```bash
python3 convert_shortlist.py --help
```  