"""
# Copied from lines 12-13 of AutoDQ/07_Anomaly Detection.py
"""
from datetime import datetime, date

"""
# Copied from lines 62-66 of AutoDQ/07_Anomaly Detection.py
"""
def convert_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


