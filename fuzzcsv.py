import sys
import os.path

version = '1.0.0'

global_filestreams = []

# Given the absolute path of a SQL file
# Performs a CSV conversion
def convert(abspath):
	sql_file_iter = read_file(abspath)
	global_filestreams.append(sql_file_iter)


######## MySQL Dump File Parsing ########
# given a read filestream fs parses it
# writes output directly to output based on path
def parse(fs, path):
	# Buffer variable for what we've already read
	read_buffer = ""
	# Dictionary of tuples of form (tablename, outstream)
	tables = {}

	for char in fs:
		read_buffer += char

		read_buffer = read_buffer[-20:] # Keep only last 20 in mem

		# read CREATE TABLE statements
		if read_buffer[-13:].upper() == "CREATE TABLE ":
			parse_create_table(fs, tables, path)
			read_buffer = "" # reset memory

		# read INSERT INTO statements
		elif read_buffer[-12:].upper() == "INSERT INTO ":
			parse_insert_into(fs, tables, path)
			read_buffer = "" #reset memory

# Parses tokens for the CREATE TABLE statment
def parse_create_table(fs, tables, path):
	tablename = parse_create_table_tablename(fs, tables)
	print("stub")

# Parses the table name for the CREATE TABLE statement
def parse_create_table_tablename(fs, tables, path):
	next_token = fs.read(1)
	if fs.read(1) != "`": # next token better be a name.
		return -1
	tablename = ""
	while next_token != "`": # iterate until name end.
		if next_token:
			tablename += next_token
		else:
			return -1 # file has terminated early
	# read success!
	add_table(fs, tables, path, tablename)

# Adds a new listing to a tables dictionary
def add_table(fs, tables, path, tablename):
	tables[tablename] = write_file(writepath(path, tablename))

# Parses tokens for the INSERT INTO statement
def parse_insert_into(fs, tables, path):
	print("stub")


########### File Manipulation ###########
# Iterator for existing SQL file at readpath
def read_file(readpath):
	return open(readpath, "r")

# Iterator for new CSV file at writepath
def write_file(writepath):
	return open(writepath, "w")

# Closes the filestream fs
def end_filestreams():
	for fs in global_filestreams:
		fs.close()

# Given a SQL file path and the current table name,
# Generates the proper write path for CSV output
def writepath(readpath, tablename):
	return readpath[:-4] + " - " + tablename + ".csv"


############# File Validity #############

# Takes a filepath and returns a tuple.
# Valid filepaths return the form (0, path)
# Invalid filepaths return the form (-1, error)
def parse_filepath(path):
	# Convert the path to an absolute path for clarity
	try:
		abspath = os.path.abspath(path)
	except:
		return (-1, "attempting to parse" + path + "generated error:\n" + sys.exc_info())

	# Use valid_filepath to see if it's okay
	valid_filepath_check = valid_filepath(abspath)
	if valid_filepath_check[0] < 10:
		return (0, valid_filepath_check[1])
	elif valid_filepath_check[0] == 10:
		return (-1, abspath + " does not exist or is inaccessible.")
	elif valid_filepath_check[0] == 11:
		return (-1, abspath + " is a directory.")
	else: # We fucked up, check valid_filepath for what code you're getting
		return (-1, abspath + "  invalid, code " + valid_filepath_check[0] + " (unknown).")

# Checks the validity of a file path. (code, path)
# Codes:
# 0 - okay, 1 - is a bad unzip but okay [okay domain: <10]
# 10 - does not exist, 11 - is a folder
def valid_filepath(abspath):
	# Let's check if it exists.
	if not os.path.exists(abspath):
		return (10, abspath)

	# Let's check if it's not a folder.
	if os.path.isdir(abspath):
		# Folder. Maybe, is it a bad unzip? Test for a subfile.
		subfile = abspath + os.sep + os.path.basename(abspath)
		subfile_validity = valid_filepath(subfile)
		if subfile_validity[0] == 0: # If subfile exists
			return (1, subfile) # This is just a crappy unizp
		else: # If subfolder, or nonexistent subfile
			return (11, abspath) # This is a folder and therefore invalid

	# exists, and is a file
	if not abspath.lower().endswith(".sql"): # if it doesn't end with .sql
		abspath = abspath + ".sql" # add .sql to the end
	return (0, abspath) # return the okay path


########## Errors and Helptext ##########

# prints the helptext for -help
def __print_help(callname):
	print()
	print("====== Fuzzworks Data Dump CSV Converter ======")
	print()
	print(" Dirty converter for the fuzzworks.co.uk MySQL")
	print(" data dumps for EVE Online. Output is CSV.")
	print(" This program's performance is not guaranteed.")
	print()
	print(" Options:")
	print("   -help          Prints this helptext.")
	print("   -ver           Reports the version number.")
	print("   -f [files]     Do not abort on bad paths.")
	print()
	print(" Usage:")
	print()
	print("  python " + callname + " -help/-ver")
	print("  python " + callname + " (options) [file1] (file2)...")
	print()
	print("---------------------------------------------")

# prints the version for -ver
def __print_ver():
	print("Release " + version)

# error message for when insufficient arguments are provided
def __throw_err_noargs(callname):
	print("Error: No arguments given. For options, launch as python " + callname + " -help")

# error message for when a bad argument is provided
def __throw_err_badpath(path, err):
	print("Error: Bad path '" + path + "', " + err)

# error message for when the program aborts
def __print_err_abort(callname):
	print("Aborting. For options, launch as python " + callname + " -help")


########### Command Interface ###########

# Main function for command-line use
def __shell():
	args = sys.argv
	callname = args[0]
	args = args[1:]

	# case: no arguments
	if len(args) == 0:
		__throw_err_noargs(callname)
		return

	# case: help flag
	if args[0].lower() == '-help':
		__print_help(callname)
		return

	# case: version check
	if args[0].lower() == '-ver':
		__print_ver()
		return

	# case: force flag check
	force = False
	if args[0].lower() == '-f':
		args = args[1:]
		force = True
		if len(args) == 0:
			__throw_err_noargs(callname)

	# otherwise interpret as .SQL file paths
	paths = []
	clean = True
	for arg in args:
		fp = parse_filepath(arg)
		if fp[0] == -1:
			__throw_err_badpath(arg, fp[1])
			clean = False
		else:
			paths.append(fp[1])

	if clean or force:
		for path in paths:
			convert(path)
	else:
		__print_err_abort(callname)
		return

# Execution
__shell()
