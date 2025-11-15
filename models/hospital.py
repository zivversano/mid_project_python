from dataclasses import dataclass
from typing import Optional

@dataclass
class Hospital:
    code:int
    name:str
    district:Optional[str]=None
    ownership:Optional[str]=None
    size:Optional[str]=None

# dictionary to map hospital codes to Hospital instances
hospital_mapping = {
    1: Hospital(code=1, name="שיבא", district="מרכז", ownership="ממשלתי", size="גדול"),
    2: Hospital(code=2, name="רמבם", district="צפון", ownership="ממשלתי", size="גדול"),
    3: Hospital(code=3, name="הדסה", district="ירושלים", ownership="ממשלתי", size="גדול"),
    4: Hospital(code=4, name="ברזילי", district="דרום", ownership="ממשלתי", size="בינוני"),
    5: Hospital(code=5, name="קפלן", district="מרכז", ownership="ממשלתי", size="בינוני"),
    6: Hospital(code    =6, name="לניאדו", district="מרכז", ownership="ממשלתי", size="בינוני"),
    7: Hospital(code=7, name="אסף הרופא", district="מרכז", ownership="ממשלתי", size="בינוני"),
    8: Hospital(code=8, name="וולפסון", district="מרכז", ownership="ממשלתי", size="בינוני"),
    9: Hospital(code=9, name="סורוקה", district="דרום", ownership="ממשלתי", size="גדול"),
    10: Hospital(code=10, name="העמק", district="צפון", ownership="ממשלתי", size="בינוני"),
    11: Hospital(code=11, name="זיו", district="צפון", ownership="ממשלתי", size="קטן"),
    12: Hospital(code=12, name="הגליל", district="צפון", ownership="ממשלתי", size="בינוני")
}

def get_hospital_name(code: int) -> Optional[str]:
    hospital = hospital_mapping.get(code)
    return hospital.name if hospital else None  
