import subprocess
import os

def check_gs_path(path):
    try:
        result = subprocess.run([path, '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Successfully found Ghostscript at {path}")
            print(f"Version: {result.stdout.strip()}")
            return True
        return False
    except Exception:
        return False

print("Checking Ghostscript installation...")

# Try default command first
try:
    result = subprocess.run(['gswin64c', '--version'], capture_output=True, text=True)
    print(f"Ghostscript version: {result.stdout}")
except FileNotFoundError:
    print("Ghostscript not found in PATH")
    print("\nChecking common installation paths...")
    
    possible_paths = [
        r'C:\Program Files\gs\gs10.02.1\bin\gswin64c.exe',
        r'C:\Program Files\gs\gs10.02.0\bin\gswin64c.exe',
        r'C:\Program Files\gs\gs10.01.2\bin\gswin64c.exe'
    ]
    
    found = False
    for path in possible_paths:
        if os.path.exists(path):
            print(f"\nFound Ghostscript executable at: {path}")
            if check_gs_path(path):
                found = True
                print("\nPlease add this path to your system's PATH environment variable:")
