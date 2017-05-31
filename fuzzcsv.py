import sys
import os.path

version = '1.0.0'

callname = "fuzzcsv.py"
global_filestreams = []

# Given the absolute path of a SQL file
# Performs a CSV conversion
def convert(abspath):
	# Initialize the reader stream for the SQL file
	sql_filestream = read_file(abspath)
	sql_fileiterator = SQLFilestreamIterator(sql_filestream)

	parse(sql_fileiterator, abspath)

	# Close all open streams
	end_filestreams()

######## MySQL Dump File Parsing ########

# Iterator for a filestream
class SQLFilestreamIterator:
	# Takes a filestream FS
	def __init__(self, fs):
		self.fs = fs
		self.next_char = fs.read(1)
		self.quoteread = self.next_char == "`" or self.next_char == "'"
		self.escape = self.next_char == "\\"

	def __iter__(self):
		return self

	# returns the next character.
	def __next__(self):
		if not self.next_char:
			raise StopIteration
		elif self.escape: # no special parsing for next char
			self.escape = False
		# special characters section
		elif self.next_char == "\\":
			self.escape = True # set explicit mode
			return self.__next__() # return the next char
		elif self.next_char == "`" or self.next_char == "'":
			self.quoteread = not self.quoteread # switch quote mode

		# iterate
		next_char = self.next_char
		self.next_char = self.fs.read(1)
		# output
		return next_char

	# returns the next token
	def next_token(self):
		token = ""
		next_char = self.next_char
		while next_char:
			if __is_tokenizable(next_char) or (self.quoteread and (next_char != "`" or next_char == "'")):
				token += next_char

			# iterate
			next_char = self.__next__()

		return token

	# if the cursor is appropriately positioned, returns the next dataset of form (data,data,data...)
	def next_data(self):
		data = []
		return data

	# returns true iff char is a valid tokenizable character (simple alphanum)
	def __is_tokenizable(char):
		if len(char) != 1:
			return False
		ordv = ord(char) # get the ordinal value of this character
		#            0-9                 A-Z                 a-z
		return (47 < ordv < 58) or (64 < ordv < 91) or (96 < ordv < 123)

# given a read filestream iterator fs parses it
# writes output directly to output based on path
def parse(fs, path):
	# Buffer variable for what we've already read
	read_buffer = ""
	# Dictionary of tuples of form (tablename, outstream)
	tables = {}
	for char in fs:
		read_buffer += char

		read_buffer = read_buffer[-20:] # Keep only last 20 chars in mem

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
	tablename = parse_create_table_tablename(fs, tables, path)
	print(tablename)

# Parses the table name for the CREATE TABLE statement
def parse_create_table_tablename(fs, tables, path):
	print("parse_create_table_tablename stub")

# Adds a new listing to a tables dictionary
def add_table(fs, tables, path, tablename):
	outpath = writepath(path, tablename)
	tables[tablename] = write_file(outpath)

# Parses tokens for the INSERT INTO statement
def parse_insert_into(fs, tables, path):
	print("parse_insert_into stub")


########### File Manipulation ###########
# Iterator for existing SQL file at readpath
def read_file(readpath):
	fs = open(readpath, "r")
	global_filestreams.append(fs)
	return fs

# Iterator for new CSV file at writepath
def write_file(writepath):
	fs = open(writepath, "w")
	global_filestreams.append(fs)
	return fs

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
def __print_help():
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
def __throw_err_noargs():
	print("Error: No arguments given. For options, launch as python " + callname + " -help")

# error message for when a bad argument is provided
def __throw_err_badpath(path, err):
	print("Error: Bad path '" + path + "', " + err)

# error message for when a file is unparseable.
def __throw_err_badfile(path, err):
	print("Error: The file " + path + " cannot be read. " + err)

# error message for when the program aborts
def __print_err_abort():
	print("Aborting. For options, launch as python " + callname + " -help")

# termination procedure for bad files
def __throw_quit_badfile(path, err):
	__throw_err_badfile(path, err)
	end_filestreams()
	__print_err_abort(callname)


########### Command Interface ###########

# Main function for command-line use
def __shell():
	args = sys.argv
	callname = args[0]
	args = args[1:]

	# case: no arguments
	if len(args) == 0:
		__throw_err_noargs()
		return

	# case: help flag
	if args[0].lower() == '-help':
		__print_help()
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
			__throw_err_noargs()

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
		__print_err_abort()
		return

# Execution
__shell()
