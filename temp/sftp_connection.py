import pysftp

myHostname = "orbweaver.com"
myUsername = "TDK-Distribution"
myPassword = "root"

with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword) as sftp:
    print("Connection succesfully stablished ... ")

    # Switch to a remote directory
    sftp.cwd('/var/www/vhosts/')

    # Obtain structure of the remote directory '/var/www/vhosts'
    directory_structure = sftp.listdir_attr()

    # Print data
    for attr in directory_structure:
        print(attr.filename, attr)