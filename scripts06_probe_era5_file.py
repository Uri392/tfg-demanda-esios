from pathlib import Path

path = Path("data/raw_era5/era5land_t2m_2006-01.nc")

data = path.read_bytes()[:32]
print("File:", path)
print("Size:", path.stat().st_size, "bytes")
print("First 32 bytes (hex):", data.hex(" "))
print("First 32 bytes (ascii):", "".join([chr(b) if 32 <= b <= 126 else "." for b in data]))
