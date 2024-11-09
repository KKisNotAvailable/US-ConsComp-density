import platform
import os

print(platform.system())

# Specify the directory
directory = '/Volumes'

# List and print all files in the directory
for filename in os.listdir(directory):
    file_path = os.path.join(directory, filename)
    if os.path.isdir(file_path):  # Check if it's a file
        print(filename)
