import os, subprocess
for cmd in (['go','version'], ['redis-server','--version']):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        print(f"{cmd[0]}: available (exit {p.returncode}); {p.stdout.strip() or p.stderr.strip()}")
    except FileNotFoundError:
        print(f"{cmd[0]}: unavailable (not found in PATH)")
    except Exception as e:
        print(f"{cmd[0]}: check failed: {type(e).__name__}: {e}")
root='/mnt/data'
found=[]
for dirpath, dirs, files in os.walk(root):
    dirs[:] = [d for d in dirs if d not in ('.cache', '__pycache__', '.git')]
    for name in files:
        if name.endswith('.go') or name in ('go.mod','go.sum'):
            found.append(os.path.join(dirpath,name))
print('Go project files:')
if found:
    for p in found: print(p)
else:
    print('none found under /mnt/data')
