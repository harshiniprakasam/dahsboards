
import subprocess
import time

scripts = [
    "IOB_Sheet1.py",
    "IOB_Sheet2.py",
    "IOB_Sheet3.py",
    "IOB_Sheet4.py",
    "IOB_Sheet5.py",
    "IOB_Sheet6.py",
    "PDF.py",
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
