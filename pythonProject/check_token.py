from config import BOT_TOKEN
from aiogram.utils.token import validate_token, TokenValidationError

print("repr:", repr(BOT_TOKEN))
print("len:", len(BOT_TOKEN))
try:
    validate_token(BOT_TOKEN)
    print("OK: формат валиден ✅")
except TokenValidationError as e:
    print("INVALID:", e)
