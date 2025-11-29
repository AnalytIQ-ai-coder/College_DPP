import bcrypt

def hpw(pw: str) -> bytes:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt())

USERS_DB = {
    "admin": hpw("admin123"),
    "john": hpw("johnpass"),
    "anna": hpw("qwerty"),
    "mike": hpw("mike2024"),
    "eva": hpw("evapass"),
    "robert": hpw("robert321"),
    "sara": hpw("sara12345"),
    "david": hpw("davidpw"),
    "julia": hpw("julia_pass"),
    "mark": hpw("mark123"),
}