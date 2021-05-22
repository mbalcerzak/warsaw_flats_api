def get_month(date: str) -> str:
    """Selects a month number from a date (String) and outputs name of the month"""
    months_pl = {
        1: "styczeń",
        2: "luty",
        3: "marzec",
        4: "kwiecień",
        5: "maj",
        6: "czerwiec",
        7: "lipiec",
        8: "sierpień",
        9: "wrzesień",
        10: "październik",
        11: "listopad",
        12: "grudzień"
    }
    return months_pl[int(date.split('-')[1])]
