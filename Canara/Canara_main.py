import subprocess
import time

scripts = [
    "Canara_sheet1.py",
    "Canara_sheet2.py",
    "Canara_sheet3.py",
    "Canara_sheet5.py",
    "Canara_sheet6.py",
    "Canarapdf.py"
]

for script in scripts:
    print(f"\n Running {script}...")
    result = subprocess.run(["python", script], capture_output=True, text=True)
    
    # Output
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"Error in {script}:\n{result.stderr}")

    # Let the OS complete any pending disk operations
    time.sleep(1)
