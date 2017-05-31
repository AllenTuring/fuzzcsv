import sys
import os.path
import time

'''
MySQL dump -> CSV converter for Fuzzworks data dumps
All blame should be attributed to Jennifer Aihaken
'''

version = '1.0.0'

callname = "fuzzcsv.py"

# list of tuples in the form (mode, stream)
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
			next_char = self.__next__() # return the next char
			return (next_char if next_char else "\\") # unless no more
		elif self.next_char == "`" or self.next_char == "'":
			self.quoteread = not self.quoteread # switch quote mode

		# iterate
		next_char = self.next_char
		self.next_char = self.fs.read(1)
		# output
		return next_char

	# returns if there is a next char.
	def has_next(self):
		return self.next_char != ''

	# returns the next token
	def next_token(self):
		token = ""
		next_char = self.next_char
		while next_char:
			# if the next character is tokenizable, or we are in quote mode and the next char isn't the leading mark,
			if self.__is_tokenizable(next_char) or (self.quoteread and not (next_char == "`" or next_char == "'")):
				token += next_char # add it to our token
			elif token: # if we can't add the next char to token, but we've written to token
				return token # output

			# iterate
			try:
				self.__next__()
				next_char = self.next_char
			except StopIteration:
				break;
		return token # end of file, return last token

	# returns the next token as datum
	def next_datum(self):
		datum = ""
		next_char = self.next_char
		while next_char:
			# if the next character is tokenizable, or we are in quote mode and the next char isn't the leading mark,
			if not next_char == ")" and not next_char == "(" and not next_char == ",":
				datum += next_char # add it to our token
			elif datum: # if we can't add the next char to token, but we've written to token
				return datum.strip() # output

			# iterate
			try:
				self.__next__()
				next_char = self.next_char
			except StopIteration:
				break;
		return datum.strip() # end of file, return last token

	# peeks the next dist chars
	def peek(self, dist):
		peek = ""
		# save current position and variables
		save = self.__peeksave()
		# write the next few chars
		while len(peek) < dist and self.next_char:
			try:
				peek += self.__next__()
			except Stopiteration:
				break;
		# reset position and variables
		self.__peekrestore(save)
		return peek

	# peeks the next dist tokens as an array
	def peek_tokens(self, dist):
		peek = []
		# save current position and variables
		save = self.__peeksave()
		# write the next few tokens
		while len(peek) < dist and self.next_char:
			peek.append(self.next_token())
		# reset position and variables
		self.__peekrestore(save)
		return peek

	# peeks to see which of a and b are next closest in the stream.
	def peek_closest(self, a, b):
		# save current position and variables
		save = self.__peeksave()
		result = self.seek_char_in((a, b))
		# reset position and variables
		self.__peekrestore(save)
		return result

	def __peeksave(self):
		# save current toggle values
		next_char = self.next_char
		quoteread = self.quoteread
		escape = self.escape
		# save the current position
		stream_posn = self.fs.tell()

		return (stream_posn, next_char, quoteread, escape)

	def __peekrestore(self, peeksave):
		#reset position
		self.fs.seek(peeksave[0])
		# reset toggle values
		self.next_char = peeksave[1]
		self.quoteread = peeksave[2]
		self.escape = peeksave[3]

	# skip to the next character matching c
	def seek_char(self, c):
		while self.next_char and self.next_char != c:
			self.__next__()

	# skip to the next character matching a member of clist
	# returns the matching character
	def seek_char_in(self, clist):
		while self.next_char and self.next_char not in clist:
			self.__next__()
		return self.next_char

	# Returns the next headerblock of form "(name type, name type, name type primary key ..)" in form [name, name, name]
	def next_headerblock(self):
		data = []
		self.seek_char("(")
		while self.next_char and " ".join(self.peek_tokens(2)).upper() != "PRIMARY KEY":
			# append the first token, name
			data.append(self.next_token())
			# iterate
			self.seek_char(",")
		return data

	# Returns the next dataset of form "(data,data,data...)" in form [data, data, data]
	def next_data(self):
		data = []
		self.seek_char("(")
		while self.next_char and self.peek_closest(",", ")") == ",":
			# append the first datum
			data.append(self.next_datum())
		return data

	# returns true iff char is a valid tokenizable character (simple alphanum + .)
	def __is_tokenizable(self, char):
		if len(char) != 1:
			return False
		ordv = ord(char) # get the ordinal value of this character
		#           .             0-9                 A-Z                  a-z
		return ordv == 46 or (47 < ordv < 58) or (64 < ordv < 91) or (96 < ordv < 123)

# given a read filestream iterator fsi parses it
# writes output directly to output based on path
def parse(fsi, path):
	print("\n >> Parsing file", path)
	start_time = time_millis()
	# Buffer variable for what we've already read
	read_buffer = ""
	# Dictionary of tuples of form (tablename, outstream)
	tables = {}
	for char in fsi:
		read_buffer += char
		read_buffer = read_buffer[-13:] # Keep only last 13 chars in mem

		# read CREATE TABLE statements
		if read_buffer[-13:].upper() == "CREATE TABLE ":
			parse_create_table(fsi, tables, path)
			read_buffer = "" # reset memory

		# read INSERT INTO statements
		elif read_buffer[-12:].upper() == "INSERT INTO ":
			parse_insert_into(fsi, tables, path)
			read_buffer = "" # reset memory

	# end report
	run_time = time_millis() - start_time
	table_count, record_count = reset_insert_writelog()
	print("\n >> Finished parsing file", path, "in", run_time/1000, "seconds.")
	print("  >> Wrote", record_count, "record(s) in", table_count, "CSV table(s), averaging", round(run_time/record_count, 3), "ms per record.\n")


# Parses tokens for the CREATE TABLE statment
def parse_create_table(fsi, tables, path):
	# get the table name, right after CREATE TABLE
	tablename = fsi.next_token()
	# get the write path for this table's CSV conversion file
	table_csv_path = writepath(path, tablename)
	# create the file and adds the listing to the tables dictionary
	add_table(tables, table_csv_path, tablename)
	print("  >> Creating table", tablename, "in file", os.path.basename(path), "as\n     ", table_csv_path,)
	# get the created CSV write filestream
	table_csv_fs = tables[tablename]

	# read headers as token-first datablock
	headers = fsi.next_headerblock()
	# write the headers into the filestream
	print("  >> Adding headers", ", ".join(headers), "to table", tablename, "in file", os.path.basename(table_csv_path))
	table_csv_fs.write(join_csv(headers))

# Adds a new listing to a tables dictionary
# Creates the filestream for CSV output for this table
def add_table(tables, path, tablename):
	if tablename in tables:
		__throw_quit_badfile(path, " duplicate table " + tablename + " in file.")
	tables[tablename] = write_file(path)

# dict of tuples of the form tablename : #records
insert_writelog = {}
# Parses tokens for the INSERT INTO statement
def parse_insert_into(fsi, tables, path):
	# get the table name, right after INSERT INTO
	tablename = fsi.next_token()
	# get the created CSV write filestream
	table_csv_fs = tables[tablename]
	# get the write path for this table's CSV conversion file (progress message only)
	table_csv_path = writepath(path, tablename)
	# start writing datablocks
	if tablename not in insert_writelog: #logging block
		insert_writelog[tablename] = 0
	datarow = insert_writelog[tablename]

	while fsi.peek_closest("(", ";") != ";": # while there are more datablocks before terminator
		datarow += 1
		if not datarow % 10000: # if modulo = 0
			print("   >> Writing data point #", datarow, "to table", tablename, "in file", os.path.basename(table_csv_path))
		table_csv_fs.write(join_csv(fsi.next_data()))

	print("   >> End of block.", datarow, "data points added to table", tablename, "in file", os.path.basename(table_csv_path), "so far.")
	insert_writelog[tablename] = datarow

# Resets the Insertion writelog and reports the total number of tables and records written.
def reset_insert_writelog():
	table_count = 0
	record_count = 0
	for key in insert_writelog:
		record_count += insert_writelog[key]
		table_count += 1
	insert_writelog.clear()
	return (table_count, record_count)

# Joins a data row for CSV output
def join_csv(data):
	return ",".join([d if ("," not in d) else ("\"" + d + "\"") for d in data]) + "\n"

########### File Manipulation ###########
# Iterator for existing SQL file at readpath
def read_file(readpath):
	try:
		fs = open(readpath, "r")
	except OSError:
		__throw_quit_badfile(readpath, " insufficient access permissions for reading.")
	global_filestreams.append(("r", fs))
	return fs

# Iterator for new CSV file at writepath
def write_file(writepath):
	try:
		fs = open(writepath, "w")
	except OSError:
		__throw_quit_badfile(readpath, " insufficient access permissions for writing.")
	global_filestreams.append(("w", fs))
	return fs

# Closes the filestream fs
def end_filestreams():
	for mode, fs in global_filestreams:
		fs.close()

# Given a SQL file path and the current table name,
# Generates the proper write path for CSV output
def writepath(readpath, tablename):
	pathname = readpath[:-4]
	suffix = ".csv"
	if os.path.basename(pathname) != tablename:
		suffix = " - " + tablename + suffix
	return pathname + suffix


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
	print(" ====== Fuzzworks Data Dump CSV Converter ======")
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
	print(" ---------------------------------------------")

# prints the version for -ver
def __print_ver():
	print("Release " + version)

# error message for when insufficient arguments are provided
def __throw_err_noargs():
	print(" !> Error: No arguments given. For options, launch as python " + callname + " -help")

# error message for when a bad argument is provided
def __throw_err_badpath(path, err):
	print(" !> Error: Bad path '" + path + "', " + err)

# error message for when a file is unparseable.
def __throw_err_badfile(path, err):
	print(" !> Error: The file " + path + " cannot be read: " + err)

# error message for when the program aborts
def __print_err_abort():
	print(" !> Aborting. For options, launch as python " + callname + " -help")

# termination procedure for bad files
def __throw_quit_badfile(path, err):
	__throw_err_badfile(path, err)
	end_filestreams()
	__print_err_abort(callname)
	exit()

# get the system time in ms
def time_millis():
	return int(time.time()*1000)

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
