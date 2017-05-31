import sys
import os.path
import io

version = '1.0.0'
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

# Execution
__shell()
