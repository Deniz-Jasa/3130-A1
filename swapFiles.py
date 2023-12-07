import shutil
import os
import getpass

# Get the current user's login name
user_name = getpass.getuser()

file_paths = {
    "createplan.c": f"/Users/{user_name}/postgresql-8.1.7/src/backend/optimizer/plan",
    "execnodes.h": f"/Users/{user_name}/postgresql-8.1.7/src/include/nodes",
    "nodeHash.c": f"/Users/{user_name}/postgresql-8.1.7/src/backend/executor",
    "nodeHashJoin.c": f"/Users/{user_name}/postgresql-8.1.7/src/backend/executor"
}

backup_dir = os.path.join(os.getcwd(), 'backup')

# Function to copy a file to a destination
def copy_file(file_name, destination):
    try:

        source_path = os.path.join(os.getcwd(), file_name)
        destination_path = os.path.join(destination, file_name)

        # Copy the file to the destination
        shutil.copy2(source_path, destination_path)
        print(f"Copied {file_name} to {destination_path}")
    except Exception as e:
        print(f"Error copying {file_name}: {e}")

def revert_files():
    for file_name, destination in file_paths.items():
        try:
            # Construct the backup file path and destination path
            backup_file_path = os.path.join(backup_dir, file_name)
            destination_path = os.path.join(destination, file_name)

            # Check if the backup file exists
            if os.path.exists(backup_file_path):
                # Replace the file at the destination with the backup file
                shutil.copy2(backup_file_path, destination_path)
                print(f"Reverted {file_name} from {backup_file_path} to {destination_path}")
            else:
                print(f"Backup file {file_name} does not exist in {backup_dir}")
        except Exception as e:
            print(f"Error reverting {file_name}: {e}")


revert_choice = input("Do you want to revert the files from backup? (y/n): ").lower().strip()
if revert_choice == 'y':
    revert_files()
else:
    for file_name, destination in file_paths.items():
        copy_file(file_name, destination)

    print("File copying process completed.")
