import os
import subprocess

if __name__ == "__main__":
    server_pid = input("kill server pid: ")

    result = subprocess.run(['kill', '-9', server_pid], capture_output=True, text=True)

    print(result.returncode)
